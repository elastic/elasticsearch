/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Requesting and returning memory from a {@link CircuitBreaker} can be costly due to the involvement of read/write
 * on one or several atomic longs. To address this issue, the local breaker adopts a strategy of over-requesting memory,
 * utilizing the reserved amount for subsequent memory requests without direct access to the actual breaker.
 *
 * @see BlockFactory#newChildFactory(LocalCircuitBreaker)
 * @see Block#allowPassingToDifferentDriver()
 */
public final class LocalCircuitBreaker implements CircuitBreaker, Releasable {
    private final CircuitBreaker breaker;
    private final long overReservedBytes;
    private final long maxOverReservedBytes;
    private long reservedBytes;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile Thread activeThread;

    public record SizeSettings(long overReservedBytes, long maxOverReservedBytes) {
        public SizeSettings(Settings settings) {
            this(
                settings.getAsBytesSize(
                    BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING,
                    BlockFactory.LOCAL_BREAKER_OVER_RESERVED_DEFAULT_SIZE
                ).getBytes(),
                settings.getAsBytesSize(
                    BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING,
                    BlockFactory.LOCAL_BREAKER_OVER_RESERVED_DEFAULT_MAX_SIZE
                ).getBytes()
            );
        }
    }

    public LocalCircuitBreaker(CircuitBreaker breaker, long overReservedBytes, long maxOverReservedBytes) {
        this.breaker = breaker;
        this.maxOverReservedBytes = maxOverReservedBytes;
        this.overReservedBytes = Math.min(overReservedBytes, maxOverReservedBytes);
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        breaker.circuitBreak(fieldName, bytesNeeded);
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        assert assertSingleThread();
        if (bytes <= reservedBytes) {
            reservedBytes -= bytes;
            maybeReduceReservedBytes();
        } else {
            breaker.addEstimateBytesAndMaybeBreak(bytes - reservedBytes + overReservedBytes, label);
            reservedBytes = overReservedBytes;
        }
    }

    @Override
    public void addWithoutBreaking(long bytes) {
        assert assertSingleThread();
        if (bytes <= reservedBytes) {
            reservedBytes -= bytes;
            maybeReduceReservedBytes();
        } else {
            // leave the reserve untouched as we are making a call anyway
            breaker.addWithoutBreaking(bytes);
        }
    }

    private void maybeReduceReservedBytes() {
        if (reservedBytes > maxOverReservedBytes) {
            breaker.addWithoutBreaking(maxOverReservedBytes - reservedBytes);
            reservedBytes = maxOverReservedBytes;
        }
    }

    public CircuitBreaker parentBreaker() {
        return breaker;
    }

    @Override
    public long getUsed() {
        return breaker.getUsed();
    }

    // for testings
    long getReservedBytes() {
        return reservedBytes;
    }

    @Override
    public long getLimit() {
        return breaker.getLimit();
    }

    @Override
    public double getOverhead() {
        return breaker.getOverhead();
    }

    @Override
    public long getTrippedCount() {
        return breaker.getTrippedCount();
    }

    @Override
    public String getName() {
        return breaker.getName();
    }

    @Override
    public Durability getDurability() {
        return breaker.getDurability();
    }

    @Override
    public void setLimitAndOverhead(long limit, double overhead) {
        breaker.setLimitAndOverhead(limit, overhead);
    }

    @Override
    public void close() {
        assert assertSingleThread();
        if (closed.compareAndSet(false, true)) {
            breaker.addWithoutBreaking(-reservedBytes);
        }
    }

    @Override
    public String toString() {
        return "LocalCircuitBreaker[" + reservedBytes + "/" + overReservedBytes + ":" + maxOverReservedBytes + "]";
    }

    private boolean assertSingleThread() {
        Thread activeThread = this.activeThread;
        Thread currentThread = Thread.currentThread();
        assert activeThread == null || activeThread == currentThread
            : "Local breaker must be accessed by a single thread at a time: expected ["
                + activeThread
                + "] != actual ["
                + currentThread
                + "]";
        return true;
    }

    /**
     * Marks the beginning of a run loop for assertion purposes.
     * Sets the current thread as the only thread allowed to access this breaker.
     */
    public boolean assertBeginRunLoop() {
        activeThread = Thread.currentThread();
        return true;
    }

    /**
     * Marks the end of a run loop for assertion purposes.
     * Clears the active thread to allow other threads to access this breaker.
     */
    public boolean assertEndRunLoop() {
        activeThread = null;
        return true;
    }
}
