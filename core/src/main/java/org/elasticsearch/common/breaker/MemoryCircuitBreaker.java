/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.breaker;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.concurrent.atomic.AtomicLong;

/**
 * MemoryCircuitBreaker is a circuit breaker that breaks once a
 * configurable memory limit has been reached.
 */
public class MemoryCircuitBreaker implements CircuitBreaker {

    private final long memoryBytesLimit;
    private final double overheadConstant;
    private final AtomicLong used;
    private final AtomicLong trippedCount;
    private final ESLogger logger;


    /**
     * Create a circuit breaker that will break if the number of estimated
     * bytes grows above the limit. All estimations will be multiplied by
     * the given overheadConstant. This breaker starts with 0 bytes used.
     * @param limit circuit breaker limit
     * @param overheadConstant constant multiplier for byte estimations
     */
    public MemoryCircuitBreaker(ByteSizeValue limit, double overheadConstant, ESLogger logger) {
        this(limit, overheadConstant, null, logger);
    }

    /**
     * Create a circuit breaker that will break if the number of estimated
     * bytes grows above the limit. All estimations will be multiplied by
     * the given overheadConstant. Uses the given oldBreaker to initialize
     * the starting offset.
     * @param limit circuit breaker limit
     * @param overheadConstant constant multiplier for byte estimations
     * @param oldBreaker the previous circuit breaker to inherit the used value from (starting offset)
     */
    public MemoryCircuitBreaker(ByteSizeValue limit, double overheadConstant, MemoryCircuitBreaker oldBreaker, ESLogger logger) {
        this.memoryBytesLimit = limit.bytes();
        this.overheadConstant = overheadConstant;
        if (oldBreaker == null) {
            this.used = new AtomicLong(0);
            this.trippedCount = new AtomicLong(0);
        } else {
            this.used = oldBreaker.used;
            this.trippedCount = oldBreaker.trippedCount;
        }
        this.logger = logger;
        if (logger.isTraceEnabled()) {
            logger.trace("Creating MemoryCircuitBreaker with a limit of {} bytes ({}) and a overhead constant of {}",
                    this.memoryBytesLimit, limit, this.overheadConstant);
        }
    }

    /**
     * Method used to trip the breaker
     */
    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) throws CircuitBreakingException {
        this.trippedCount.incrementAndGet();
        final String message = "Data too large, data for field [" + fieldName + "] would be larger than limit of [" +
                memoryBytesLimit + "/" + new ByteSizeValue(memoryBytesLimit) + "]";
        logger.debug("{}", message);
        throw new CircuitBreakingException(message, bytesNeeded, memoryBytesLimit);
    }

    /**
     * Add a number of bytes, tripping the circuit breaker if the aggregated
     * estimates are above the limit. Automatically trips the breaker if the
     * memory limit is set to 0. Will never trip the breaker if the limit is
     * set &lt; 0, but can still be used to aggregate estimations.
     * @param bytes number of bytes to add to the breaker
     * @return number of "used" bytes so far
     */
    @Override
    public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        // short-circuit on no data allowed, immediately throwing an exception
        if (memoryBytesLimit == 0) {
            circuitBreak(label, bytes);
        }

        long newUsed;
        // If there is no limit (-1), we can optimize a bit by using
        // .addAndGet() instead of looping (because we don't have to check a
        // limit), which makes the RamAccountingTermsEnum case faster.
        if (this.memoryBytesLimit == -1) {
            newUsed = this.used.addAndGet(bytes);
            if (logger.isTraceEnabled()) {
                logger.trace("Adding [{}][{}] to used bytes [new used: [{}], limit: [-1b]]",
                        new ByteSizeValue(bytes), label, new ByteSizeValue(newUsed));
            }
            return newUsed;
        }

        // Otherwise, check the addition and commit the addition, looping if
        // there are conflicts. May result in additional logging, but it's
        // trace logging and shouldn't be counted on for additions.
        long currentUsed;
        do {
            currentUsed = this.used.get();
            newUsed = currentUsed + bytes;
            long newUsedWithOverhead = (long)(newUsed * overheadConstant);
            if (logger.isTraceEnabled()) {
                logger.trace("Adding [{}][{}] to used bytes [new used: [{}], limit: {} [{}], estimate: {} [{}]]",
                        new ByteSizeValue(bytes), label, new ByteSizeValue(newUsed),
                        memoryBytesLimit, new ByteSizeValue(memoryBytesLimit),
                        newUsedWithOverhead, new ByteSizeValue(newUsedWithOverhead));
            }
            if (memoryBytesLimit > 0 && newUsedWithOverhead > memoryBytesLimit) {
                logger.warn("New used memory {} [{}] from field [{}] would be larger than configured breaker: {} [{}], breaking",
                        newUsedWithOverhead, new ByteSizeValue(newUsedWithOverhead), label,
                        memoryBytesLimit, new ByteSizeValue(memoryBytesLimit));
                circuitBreak(label, newUsedWithOverhead);
            }
            // Attempt to set the new used value, but make sure it hasn't changed
            // underneath us, if it has, keep trying until we are able to set it
        } while (!this.used.compareAndSet(currentUsed, newUsed));

        return newUsed;
    }

    /**
     * Add an <b>exact</b> number of bytes, not checking for tripping the
     * circuit breaker. This bypasses the overheadConstant multiplication.
     * @param bytes number of bytes to add to the breaker
     * @return number of "used" bytes so far
     */
    @Override
    public long addWithoutBreaking(long bytes) {
        long u = used.addAndGet(bytes);
        if (logger.isTraceEnabled()) {
            logger.trace("Adjusted breaker by [{}] bytes, now [{}]", bytes, u);
        }
        assert u >= 0 : "Used bytes: [" + u + "] must be >= 0";
        return u;
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
        return this.memoryBytesLimit;
    }

    /**
     * @return the constant multiplier the breaker uses for aggregations
     */
    @Override
    public double getOverhead() {
        return this.overheadConstant;
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
        return FIELDDATA;
    }
}
