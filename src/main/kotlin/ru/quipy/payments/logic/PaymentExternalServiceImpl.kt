package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    override val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        const val MAX_ATTEMPTS = 6
    }

    private data class AttemptResult(val success: Boolean, val retry: Boolean, val outcome: String)

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().readTimeout(Duration.ofSeconds(60)).build()
    private val rateLimiter = LeakingBucketRateLimiter(
        (rateLimitPerSec - 1).coerceAtLeast(1).toLong(), Duration.ofSeconds(1), parallelRequests
    )
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val callCounters = ConcurrentHashMap<String, Counter>()
    private val callTimers = ConcurrentHashMap<String, Timer>()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val budget = Duration.ofMillis((deadline - paymentStartedAt).coerceAtLeast(0))
        val attemptTimeout = maxOf(requestAverageProcessingTime.multipliedBy(5).dividedBy(4), budget.dividedBy(4))

        var attempt = 1
        var result: AttemptResult
        while (true) {
            result = performPaymentAttempt(paymentId, amount, paymentStartedAt, deadline, attemptTimeout, attempt)
            if (!result.retry || attempt >= MAX_ATTEMPTS || now() + requestAverageProcessingTime.toMillis() >= deadline) break
            attempt++
        }
        meterRegistry.counter(
            "shop.payment.attempts", "service", serviceName, "account", accountName,
            "result", if (result.success) "success" else "failed", "attempts", attempt.toString()
        ).increment()
    }

    private fun performPaymentAttempt(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        attemptTimeout: Duration,
        attempt: Int,
    ): AttemptResult {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId, attempt $attempt")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        ongoingWindow.acquire()
        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val wait = rateLimiter.reserve()
            if (!wait.isZero) TimeUnit.NANOSECONDS.sleep(wait.toNanos())
            if (now() >= deadline) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Payment deadline expired before sending.")
                }
                return AttemptResult(success = false, retry = false, outcome = "not_sent")
            }

            val startedAt = System.nanoTime()
            val result: AttemptResult = try {
                val call = client.newCall(request)
                call.timeout().timeout(minOf(attemptTimeout.toMillis(), deadline - now()).coerceAtLeast(1), TimeUnit.MILLISECONDS)
                call.execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, attempt $attempt, succeeded: ${body.result}, message: ${body.message}")

                    val outcome = when {
                        body.result -> "success"
                        response.code == 408 || body.message == "Timeout" -> "timeout"
                        body.message == "Temporary error" -> "temporary_error"
                        response.code == 429 -> "rate_limited"
                        response.code == 509 -> "parallel_limited"
                        else -> "rejected"
                    }

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    AttemptResult(success = body.result, retry = outcome != "success" && outcome != "rejected", outcome = outcome)
                }
            } catch (e: InterruptedIOException) {
                logger.warn("[$accountName] Payment attempt $attempt timed out for txId: $transactionId, payment: $paymentId")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
                AttemptResult(success = false, retry = true, outcome = "timeout")
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
                AttemptResult(success = false, retry = true, outcome = "error")
            }
            callCounter(result.outcome).increment()
            callTimer(result.outcome).record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS)
            return result
        } finally {
            ongoingWindow.release()
        }
    }

    // Every call to the payment system costs its price, so these counters are the money spent.
    private fun callCounter(outcome: String) = callCounters.computeIfAbsent(outcome) {
        meterRegistry.counter("shop.payment.provider.calls", "service", serviceName, "account", accountName, "outcome", it)
    }

    private fun callTimer(outcome: String) = callTimers.computeIfAbsent(outcome) {
        Timer.builder("shop.payment.provider.call")
            .tag("service", serviceName).tag("account", accountName).tag("outcome", it)
            .register(meterRegistry)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
