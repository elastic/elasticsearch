/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.breaker.CircuitBreaker;

import java.util.Objects;

/**
 * Profile-only estimate of explicitly admitted state and block data currently held by one SEQUENCE source.
 * The caller supplies ownership deltas, not reference-count changes or shared breaker totals. Planning, transient
 * child/evaluator/builder work and downstream ownership are outside this estimate. It enforces no new memory limit.
 */
final class SequenceHeldMemory {
    private long currentBytes;
    private long peakBytes;

    synchronized long currentBytes() {
        return currentBytes;
    }

    synchronized long peakBytes() {
        return peakBytes;
    }

    /** A scope groups allocations with one ownership boundary, such as pending state or an outgoing result page. */
    Scope newScope() {
        return new Scope();
    }

    /** An outgoing scope may transfer ownership without freeing the underlying allocation or its real breaker charge. */
    final class Scope {
        private long bytes;
        private volatile boolean detached;

        /**
         * Observes explicit breaker admissions before any local reservation pooling. The delegate's threading contract
         * remains in force: wrapping a driver-local breaker does not make it safe for asynchronous callbacks.
         */
        CircuitBreaker breaker(CircuitBreaker delegate) {
            Objects.requireNonNull(delegate);
            if (delegate instanceof AccountingBreaker) {
                throw new IllegalArgumentException("a sequence accounting breaker must not wrap another accounting breaker");
            }
            return new AccountingBreaker(this, delegate);
        }

        /** Observes held block RAM without adding another real breaker charge for that already-accounted block. */
        void recordHeldBytes(long delta) {
            account(delta, () -> {});
        }

        /**
         * Runs exactly one underlying accounting operation, recording its delta only when it succeeds. The operation
         * must not call another hook on this tracker; each allocation must have one accounting hook.
         */
        void account(long delta, Runnable accountingOperation) {
            if (detached) {
                accountingOperation.run();
                return;
            }
            synchronized (SequenceHeldMemory.this) {
                if (detached) {
                    accountingOperation.run();
                    return;
                }
                long nextScopeBytes = Math.addExact(bytes, delta);
                long nextCurrentBytes = Math.addExact(currentBytes, delta);
                if (nextScopeBytes < 0 || nextCurrentBytes < 0) {
                    throw new IllegalStateException("sequence owned-memory release exceeds the scope's live admissions");
                }
                accountingOperation.run();
                bytes = nextScopeBytes;
                currentBytes = nextCurrentBytes;
                peakBytes = Math.max(peakBytes, currentBytes);
            }
        }

        /**
         * Stops attributing this scope to the source. Later physical releases still reach the real breaker through
         * its wrappers. Call this at a genuine ownership transfer, never as a replacement for resource cleanup.
         */
        void detach() {
            synchronized (SequenceHeldMemory.this) {
                if (detached == false) {
                    currentBytes -= bytes;
                    bytes = 0;
                    detached = true;
                }
            }
        }
    }

    /** Delegates the real breaker policy unchanged while exposing only this scope's successful allocation deltas. */
    private static final class AccountingBreaker implements CircuitBreaker {
        private final SequenceHeldMemory.Scope scope;
        private final CircuitBreaker delegate;

        private AccountingBreaker(SequenceHeldMemory.Scope scope, CircuitBreaker delegate) {
            this.scope = scope;
            this.delegate = delegate;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            delegate.circuitBreak(fieldName, bytesNeeded);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            scope.account(bytes, () -> delegate.addEstimateBytesAndMaybeBreak(bytes, label));
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            scope.account(bytes, () -> delegate.addWithoutBreaking(bytes));
        }

        @Override
        public void addWithoutBreaking(long bytes, String label) {
            scope.account(bytes, () -> delegate.addWithoutBreaking(bytes, label));
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
