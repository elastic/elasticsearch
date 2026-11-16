/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

/**
 * Always-active admission limit for the estimates owned by one SEQUENCE source. Explicit state and source-created
 * blocks/buffers use scoped request breakers; ordinary child pages join the budget when the source accepts them.
 * Borrowed evaluator scratch and downstream ownership remain outside this source limit. Distinct foreign block
 * wrappers sharing storage can be charged conservatively; this is not an exact measure of whole-query heap.
 */
final class SequenceMemoryBudget {
    private final long limit;
    private long currentBytes;
    private long peakBytes;

    SequenceMemoryBudget(long limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("SEQUENCE memory limit must be nonnegative");
        }
        this.limit = limit;
    }

    synchronized long currentBytes() {
        return currentBytes;
    }

    synchronized long peakBytes() {
        return peakBytes;
    }

    Scope newScope() {
        return new Scope();
    }

    /** Child blocks already have real request charges; this admission covers only their source ownership. */
    synchronized void admitForeign(long bytes) {
        check(currentBytes + bytes);
        currentBytes = Math.addExact(currentBytes, bytes);
        peakBytes = Math.max(peakBytes, currentBytes);
    }

    synchronized void releaseForeign(long bytes) {
        if (bytes < 0 || bytes > currentBytes) {
            throw new IllegalStateException("SEQUENCE foreign-page release exceeds live admissions");
        }
        currentBytes -= bytes;
    }

    /** Do not charge the same source-created block again merely because a stage forwards its reference. */
    long foreignBytes(Page page) {
        long bytes = 0;
        for (int channel = 0; channel < page.getBlockCount(); channel++) {
            Block block = page.getBlock(channel);
            bytes = Math.addExact(bytes, foreignBytes(block));
        }
        return bytes;
    }

    /** An evaluator may return a new block or forward a source-owned cell; admit only the former. */
    long foreignBytes(Block block) {
        CircuitBreaker breaker = LocalCircuitBreaker.forAsyncIo(block.blockFactory().breaker());
        return breaker instanceof BudgetBreaker scoped && scoped.owner() == this ? 0 : block.ramBytesUsed();
    }

    static long pageBytes(int columns) {
        // Page, reference array and overlap while constructing or appending a page.
        return 96L + 16L * columns;
    }

    private void check(long next) {
        if (next < 0 || next > limit) {
            throw new CircuitBreakingException(
                "SEQUENCE memory limit ["
                    + PlannerSettings.SEQUENCE_MAX_MEMORY.getKey()
                    + "] exceeded: attempted ["
                    + next
                    + "] bytes, limit ["
                    + limit
                    + "] bytes",
                next,
                limit,
                CircuitBreaker.Durability.TRANSIENT
            );
        }
    }

    /** A real ownership boundary can detach, leaving its eventual physical refunds with the request breaker. */
    final class Scope {
        private long bytes;
        private boolean detached;

        CircuitBreaker breaker(CircuitBreaker delegate) {
            return new BudgetBreaker(this, delegate);
        }

        /** Preserve the parent's allocator, recycling and block sizing; only this source's admission policy differs. */
        BlockFactory blockFactory(BlockFactory parent) {
            CircuitBreaker scoped = breaker(LocalCircuitBreaker.forAsyncIo(parent.breaker()));
            var arrays = parent.bigArrays().withBreakerService(new CircuitBreakerService() {
                @Override
                public CircuitBreaker getBreaker(String name) {
                    return scoped;
                }

                @Override
                public AllCircuitBreakerStats stats() {
                    throw new UnsupportedOperationException("source-local breaker service has no global stats");
                }

                @Override
                public CircuitBreakerStats stats(String name) {
                    throw new UnsupportedOperationException("source-local breaker service has no global stats");
                }
            }).withCircuitBreaking();
            return BlockFactory.builder(arrays)
                .breaker(scoped)
                .maxPrimitiveArraySize(parent.maxPrimitiveArrayBytes())
                .bytesRefRamOverestimateThreshold(parent.bytesRefRamOverestimateThreshold())
                .bytesRefRamOverestimateFactor(parent.bytesRefRamOverestimateFactor())
                .build();
        }

        private void account(long delta, boolean compensation, Runnable operation) {
            synchronized (SequenceMemoryBudget.this) {
                if (detached) {
                    operation.run();
                    return;
                }
                long nextScope = Math.addExact(bytes, delta);
                long nextTotal = Math.addExact(currentBytes, delta);
                if (nextScope < 0 || nextTotal < 0) {
                    throw new IllegalStateException("SEQUENCE release exceeds live admissions");
                }
                if (delta > 0 && compensation == false) {
                    check(nextTotal);
                }
                operation.run();
                bytes = nextScope;
                currentBytes = nextTotal;
                peakBytes = Math.max(peakBytes, nextTotal);
            }
        }

        void detach() {
            synchronized (SequenceMemoryBudget.this) {
                if (detached == false) {
                    currentBytes -= bytes;
                    bytes = 0;
                    detached = true;
                }
            }
        }
    }

    /** All ordinary positive deltas are admissions, including positive addWithoutBreaking calls. */
    private final class BudgetBreaker implements CircuitBreaker {
        private final Scope scope;
        private final CircuitBreaker delegate;
        private final ThreadLocal<Long> rejected = new ThreadLocal<>();

        private BudgetBreaker(Scope scope, CircuitBreaker delegate) {
            this.scope = scope;
            this.delegate = delegate;
        }

        private SequenceMemoryBudget owner() {
            return SequenceMemoryBudget.this;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            delegate.circuitBreak(fieldName, bytesNeeded);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            rejected.remove();
            try {
                scope.account(bytes, false, () -> delegate.addEstimateBytesAndMaybeBreak(bytes, label));
            } catch (CircuitBreakingException failure) {
                // BigArrays validates some already-created arrays. It then accounts exactly this rejected delta
                // without breaking, solely so its failure cleanup can refund it without masking this exception.
                if (bytes > 0) {
                    rejected.set(bytes);
                }
                throw failure;
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            Long failed = rejected.get();
            rejected.remove();
            scope.account(bytes, bytes > 0 && failed != null && failed == bytes, () -> delegate.addWithoutBreaking(bytes));
        }

        @Override
        public void addWithoutBreaking(long bytes, String label) {
            addWithoutBreaking(bytes);
        }

        @Override
        public long getUsed() {
            return delegate.getUsed();
        }

        @Override
        public long getLimit() {
            return delegate.getLimit();
        }

        @Override
        public double getOverhead() {
            return delegate.getOverhead();
        }

        @Override
        public long getTrippedCount() {
            return delegate.getTrippedCount();
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public Durability getDurability() {
            return delegate.getDurability();
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            delegate.setLimitAndOverhead(limit, overhead);
        }
    }
}
