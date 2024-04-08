/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.breaker;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.telemetry.metric.LongCounter;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.core.Strings.format;

/**
 * Breaker that will check a parent's when incrementing
 */
public class ChildMemoryCircuitBreaker implements CircuitBreaker {

    private volatile LimitAndOverhead limitAndOverhead;
    private final Durability durability;
    private final AtomicLong used;
    private final AtomicLong trippedCount;
    private final Logger logger;
    private final HierarchyCircuitBreakerService parent;
    private final String name;
    private final LongCounter trippedCountMeter;

    public static final String CIRCUIT_BREAKER_TYPE_ATTRIBUTE = "type";

    /**
     * Create a circuit breaker that will break if the number of estimated
     * bytes grows above the limit. All estimations will be multiplied by
     * the given overheadConstant. Uses the given oldBreaker to initialize
     * the starting offset.
     * @param trippedCountMeter the counter used to report the tripped count metric
     * @param settings settings to configure this breaker
     * @param parent parent circuit breaker service to delegate tripped breakers to
     * @param name the name of the breaker
     */
    public ChildMemoryCircuitBreaker(
        LongCounter trippedCountMeter,
        BreakerSettings settings,
        Logger logger,
        HierarchyCircuitBreakerService parent,
        String name
    ) {
        this.name = name;
        this.limitAndOverhead = new LimitAndOverhead(settings.getLimit(), settings.getOverhead());
        this.durability = settings.getDurability();
        this.used = new AtomicLong(0);
        this.trippedCount = new AtomicLong(0);
        this.logger = logger;
        logger.trace(() -> format("creating ChildCircuitBreaker with settings %s", settings));
        this.parent = parent;
        this.trippedCountMeter = trippedCountMeter;
    }

    /**
     * Method used to trip the breaker, delegates to the parent to determine
     * whether to trip the breaker or not
     */
    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        final long memoryBytesLimit = this.limitAndOverhead.limit;
        this.trippedCount.incrementAndGet();
        this.trippedCountMeter.incrementBy(1L, Collections.singletonMap(CIRCUIT_BREAKER_TYPE_ATTRIBUTE, this.name));
        final String message = "["
            + this.name
            + "] Data too large, data for ["
            + fieldName
            + "]"
            + " would be ["
            + bytesNeeded
            + "/"
            + ByteSizeValue.ofBytes(bytesNeeded)
            + "]"
            + ", which is larger than the limit of ["
            + memoryBytesLimit
            + "/"
            + ByteSizeValue.ofBytes(memoryBytesLimit)
            + "]";
        logger.debug(() -> format("%s", message));
        throw new CircuitBreakingException(message, bytesNeeded, memoryBytesLimit, durability);
    }

    /**
     * Add a number of bytes, tripping the circuit breaker if the aggregated
     * estimates are above the limit. Automatically trips the breaker if the
     * memory limit is set to 0. Will never trip the breaker if the limit is
     * set to -1, but can still be used to aggregate estimations.
     * @param bytes number of bytes to add to the breaker
     */
    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        final LimitAndOverhead limitAndOverhead = this.limitAndOverhead;
        final long memoryBytesLimit = limitAndOverhead.limit;
        final double overheadConstant = limitAndOverhead.overhead;
        // short-circuit on no data allowed, immediately throwing an exception
        if (memoryBytesLimit == 0) {
            circuitBreak(label, bytes);
        }

        long newUsed;
        // If there is no limit (-1), we can optimize a bit by using
        // .addAndGet() instead of looping (because we don't have to check a
        // limit), which makes the RamAccountingTermsEnum case faster.
        if (memoryBytesLimit == -1) {
            newUsed = noLimit(bytes, label);
        } else {
            newUsed = limit(bytes, label, overheadConstant, memoryBytesLimit);
        }

        // Additionally, we need to check that we haven't exceeded the parent's limit
        try {
            parent.checkParentLimit((long) (bytes * overheadConstant), label);
        } catch (CircuitBreakingException e) {
            // If the parent breaker is tripped, this breaker has to be
            // adjusted back down because the allocation is "blocked" but the
            // breaker has already been incremented
            this.addWithoutBreaking(-bytes);
            throw e;
        }
        assert newUsed >= 0 : "Used bytes: [" + newUsed + "] must be >= 0";
    }

    private long noLimit(long bytes, String label) {
        long newUsed;
        newUsed = this.used.addAndGet(bytes);
        logger.trace(
            () -> format(
                "[%s] Adding [%s][%s] to used bytes [new used: [%s], limit: [-1b]]",
                this.name,
                ByteSizeValue.ofBytes(bytes),
                label,
                ByteSizeValue.ofBytes(newUsed)
            )
        );
        return newUsed;
    }

    private long limit(long bytes, String label, double overheadConstant, long memoryBytesLimit) {
        long newUsed;// Otherwise, check the addition and commit the addition, looping if
        // there are conflicts. May result in additional logging, but it's
        // trace logging and shouldn't be counted on for additions.
        long currentUsed;
        do {
            currentUsed = this.used.get();
            newUsed = currentUsed + bytes;
            long newUsedWithOverhead = (long) (newUsed * overheadConstant);
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "[{}] Adding [{}][{}] to used bytes [new used: [{}], limit: {} [{}], estimate: {} [{}]]",
                    this.name,
                    ByteSizeValue.ofBytes(bytes),
                    label,
                    ByteSizeValue.ofBytes(newUsed),
                    memoryBytesLimit,
                    ByteSizeValue.ofBytes(memoryBytesLimit),
                    newUsedWithOverhead,
                    ByteSizeValue.ofBytes(newUsedWithOverhead)
                );
            }
            if (memoryBytesLimit > 0 && newUsedWithOverhead > memoryBytesLimit) {
                logger.warn(
                    "[{}] New used memory {} [{}] for data of [{}] would be larger than configured breaker: {} [{}], breaking",
                    this.name,
                    newUsedWithOverhead,
                    ByteSizeValue.ofBytes(newUsedWithOverhead),
                    label,
                    memoryBytesLimit,
                    ByteSizeValue.ofBytes(memoryBytesLimit)
                );
                circuitBreak(label, newUsedWithOverhead);
            }
            // Attempt to set the new used value, but make sure it hasn't changed
            // underneath us, if it has, keep trying until we are able to set it
        } while (this.used.compareAndSet(currentUsed, newUsed) == false);
        return newUsed;
    }

    /**
     * Add an <b>exact</b> number of bytes, not checking for tripping the
     * circuit breaker. This bypasses the overheadConstant multiplication.
     *
     * Also does not check with the parent breaker to see if the parent limit
     * has been exceeded.
     *
     * @param bytes number of bytes to add to the breaker
     */
    @Override
    public void addWithoutBreaking(long bytes) {
        long u = used.addAndGet(bytes);
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] Adjusted breaker by [{}] bytes, now [{}]", this.name, bytes, u);
        }
        assert u >= 0 : "Used bytes: [" + u + "] must be >= 0";
    }

    /**
     * @return the number of aggregated "used" bytes so far
     */
    @Override
    public long getUsed() {
        return this.used.get();
    }

    /**
     * @return the number of bytes that can be added before the breaker trips
     */
    @Override
    public long getLimit() {
        return this.limitAndOverhead.limit;
    }

    /**
     * @return the constant multiplier the breaker uses for aggregations
     */
    @Override
    public double getOverhead() {
        return this.limitAndOverhead.overhead;
    }

    /**
     * @return the number of times the breaker has been tripped
     */
    @Override
    public long getTrippedCount() {
        return this.trippedCount.get();
    }

    /**
     * @return the name of the breaker
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * @return whether a tripped circuit breaker will reset itself (transient) or requires manual intervention (permanent).
     */
    @Override
    public Durability getDurability() {
        return this.durability;
    }

    @Override
    public void setLimitAndOverhead(long limit, double overhead) {
        this.limitAndOverhead = new LimitAndOverhead(limit, overhead);
    }

    private static class LimitAndOverhead {

        private final long limit;
        private final double overhead;

        LimitAndOverhead(long limit, double overhead) {
            this.limit = limit;
            this.overhead = overhead;
        }
    }
}
