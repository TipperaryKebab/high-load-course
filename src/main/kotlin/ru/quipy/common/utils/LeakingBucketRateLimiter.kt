package ru.quipy.common.utils

import java.time.Duration
import kotlin.math.ceil

class LeakingBucketRateLimiter(
    rate: Long,
    window: Duration,
    bucketSize: Int,
) : RateLimiter {
    init {
        require(rate > 0 && !window.isNegative && !window.isZero && bucketSize > 0)
    }

    private val intervalNanos = ceil(window.toNanos().toDouble() / rate).toLong()
    private val capacityDelayNanos = (bucketSize - 1L) * intervalNanos
    private var nextPermitAt = System.nanoTime()

    override fun tick() = tick(Duration.ofNanos(capacityDelayNanos))

    @Synchronized
    fun tick(maxWait: Duration): Boolean {
        val now = System.nanoTime()
        val wait = (nextPermitAt - now).coerceAtLeast(0)
        if (wait > minOf(maxWait.toNanos(), capacityDelayNanos)) return false
        nextPermitAt = now + wait + intervalNanos
        return true
    }
}
