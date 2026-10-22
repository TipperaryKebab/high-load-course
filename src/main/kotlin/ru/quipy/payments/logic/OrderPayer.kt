package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import jakarta.annotation.PreDestroy
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.apigateway.APIController.OrderStatus
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.ceil

@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    private val orderRepository: OrderRepository,
    paymentAccounts: List<PaymentExternalSystemAdapter>,
    meterRegistry: MeterRegistry,
    @Value("\${payment.service-name}") serviceName: String,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    private val accounts = paymentAccounts.map { it.properties }.also { accounts ->
        require(accounts.isNotEmpty())
        require(accounts.all { it.rateLimitPerSec > 0 && it.parallelRequests > 0 })
    }
    private val averageProcessingMillis = accounts.sumOf { it.averageProcessingTime.toMillis().coerceAtLeast(1) }
    private val processingRate = accounts.minOf {
        minOf(it.rateLimitPerSec / 1.1, it.parallelRequests * 1000.0 / it.averageProcessingTime.toMillis().coerceAtLeast(1))
    }
    private val workers = ceil(processingRate * averageProcessingMillis / 1000).toInt()
        .coerceIn(1, accounts.minOf { it.parallelRequests })
    val retryAfterMillis = ceil(1000 / processingRate).toLong()

    private val paymentExecutor = ThreadPoolExecutor(
        workers,
        workers,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(ceil(processingRate).toInt()),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    init {
        ExecutorServiceMetrics(paymentExecutor, "payment-submission-executor", Tags.of("service", serviceName))
            .bindTo(meterRegistry)
    }

    @Synchronized
    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()
        val estimatedWaitMillis = (paymentExecutor.queue.size + 1) * 1000.0 / processingRate
        if (estimatedWaitMillis + 2 * averageProcessingMillis >= deadline - createdAt) return null

        try {
            paymentExecutor.execute {
                val order = orderRepository.findById(orderId) ?: throw IllegalArgumentException("No such order $orderId")
                orderRepository.save(order.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                if (now() >= deadline) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), reason = "Payment deadline expired in queue.")
                    }
                    return@execute
                }
                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            }
        } catch (_: RejectedExecutionException) {
            return null
        }
        return createdAt
    }

    @PreDestroy
    fun shutdown() = paymentExecutor.shutdown()
}
