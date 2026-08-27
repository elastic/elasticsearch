/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.breaker;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;

/**
 * {@link CircuitBreakerService} that preallocates some bytes on construction.
 * Use this when you know you'll be allocating many small things on a
 * {@link CircuitBreaker} quickly and there is a definite "finished" time, like
 * when aggregations are built.
 */
public class PreallocatedCircuitBreakerService extends CircuitBreakerService implements Releasable {
    private final CircuitBreakerService next;
    private final PreallocatedCircuitBreaker preallocated;

    public PreallocatedCircuitBreakerService(
        CircuitBreakerService next,
        String breakerToPreallocate,
        long bytesToPreallocate,
        String label
    ) {
        if (bytesToPreallocate <= 0) {
            throw new IllegalArgumentException("can't preallocate negative or zero bytes but got [" + bytesToPreallocate + "]");
        }
        CircuitBreaker nextBreaker = next.getBreaker(breakerToPreallocate);
        final String preallocateLabel = ChildMemoryCircuitBreaker.CATEGORY_PREALLOCATE + "[" + label + "]";
        nextBreaker.addEstimateBytesAndMaybeBreak(bytesToPreallocate, preallocateLabel);
        this.next = next;
        this.preallocated = new PreallocatedCircuitBreaker(nextBreaker, bytesToPreallocate, preallocateLabel);
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        if (name.equals(preallocated.getName())) {
            return preallocated;
        }
        return next.getBreaker(name);
    }

    @Override
    public AllCircuitBreakerStats stats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        preallocated.close();
    }

    /**
     * The preallocated breaker.
     * <p>
     * This breaker operates in two states:
     * <ol>
     * <li>We've used fewer bytes than we've preallocated.
     * <li>We've used all of the preallocated bytes.
     * </ol>
     * <p>
     * If we're in the "used fewer bytes" state than we've allocated then
     * allocating new bytes just adds to
     * {@link PreallocatedCircuitBreaker#preallocationUsed}, maxing out at
     * {@link PreallocatedCircuitBreaker#preallocated}. If we max
     * out we irreversibly switch to "used all" state. In that state any
     * additional allocations are passed directly to the underlying breaker.
     * <p>
     * De-allocating is just allocating a negative number of bytes. De-allocating
     * can not transition us from the "used all" state back into the
     * "used fewer bytes" state. It is a one way trip. Once we're in the
     * "used all" state all de-allocates are done directly on the underlying
     * breaker. So well behaved callers will naturally de-allocate everything.
     * <p>
     * {@link PreallocatedCircuitBreaker#close()} is only used to de-allocate
     * bytes from the underlying breaker if we're still in the "used fewer bytes"
     * state. There is nothing to de-allocate if we are in the "used all" state.
     */
    private static class PreallocatedCircuitBreaker implements CircuitBreaker, Releasable {
        private final CircuitBreaker next;
        private final long preallocated;
        private final String preallocateLabel;
        private long preallocationUsed;
        private boolean closed;

        PreallocatedCircuitBreaker(CircuitBreaker next, long preallocated, String preallocateLabel) {
            this.next = next;
            this.preallocated = preallocated;
            this.preallocateLabel = preallocateLabel;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            next.circuitBreak(fieldName, bytesNeeded);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (closed) {
                throw new IllegalStateException("already closed");
            }
            if (preallocationUsed == preallocated || bytes == 0L) {
                // Preallocation buffer was full before this request or we are checking the parent circuit breaker
                next.addEstimateBytesAndMaybeBreak(bytes, label);
                return;
            }
            long newUsed = preallocationUsed + bytes;
            if (newUsed > preallocated) {
                // This request filled up the buffer
                next.addEstimateBytesAndMaybeBreak(newUsed - preallocated, label);
                // Adjust preallocationUsed only after we know we didn't circuit break!
                preallocationUsed = preallocated;
                return;
            }
            // This is the fast case. No volatile reads or writes here, ma!
            preallocationUsed = newUsed;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            addWithoutBreakingInternal(bytes, ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED);
        }

        @Override
        public void addWithoutBreaking(long bytes, String label) {
            addWithoutBreakingInternal(bytes, label);
        }

        private void addWithoutBreakingInternal(long bytes, String label) {
            if (closed) {
                throw new IllegalStateException("already closed");
            }
            if (preallocationUsed == preallocated) {
                // Preallocation buffer was full before this request
                next.addWithoutBreaking(bytes, label);
                return;
            }
            long newUsed = preallocationUsed + bytes;
            if (newUsed > preallocated) {
                // This request filled up the buffer
                preallocationUsed = preallocated;
                next.addWithoutBreaking(newUsed - preallocated, label);
                return;
            }
            // This is the fast case. No volatile reads or writes here, ma!
            preallocationUsed = newUsed;
        }

        @Override
        public String getName() {
            return next.getName();
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            if (preallocationUsed < preallocated) {
                /*
                 * "Used fewer bytes" state: the underlying breaker still holds the whole preallocated reservation
                 * (callers only ever touched the in-memory buffer), so give those real bytes back. Reusing the
                 * construction label keeps both the real used bytes and the per-category
                 * es.breaker.memory.held.usage gauge balanced - the admit and this release cancel out under
                 * category="preallocate[<label>]".
                 */
                next.addWithoutBreaking(-preallocated, preallocateLabel);
            } else {
                /*
                 * "Used all" state: this is a one-way transition, after which every operation - including
                 * deallocations - hits the underlying breaker directly. Well-behaved callers therefore already
                 * returned the preallocated bytes to the real breaker, so we must NOT release real bytes again.
                 *
                 * The held gauge, however, still carries the +preallocated admit recorded under
                 * category="preallocate[<label>]" at construction; left alone it would accumulate a permanent
                 * positive per preallocation. Cancel just that gauge entry by re-bucketing the reservation into
                 * the uncategorized category. We use recordHeldDelta rather than addWithoutBreaking to avoid
                 * transiently inflating the real used counter between the two corrections, which could otherwise
                 * cause a concurrent checkParentLimit call to see a spuriously high value and trip the parent.
                 */
                if (next instanceof ChildMemoryCircuitBreaker cb) {
                    cb.recordHeldDelta(preallocated, ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED);
                    cb.recordHeldDelta(-preallocated, preallocateLabel);
                } else {
                    next.addWithoutBreaking(-preallocated, preallocateLabel);
                    next.addWithoutBreaking(preallocated, ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED);
                }
            }
            closed = true;
        }

        @Override
        public long getUsed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLimit() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getOverhead() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTrippedCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Durability getDurability() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            throw new UnsupportedOperationException();
        }
    }
}
