/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.shard.ShardLongFieldRange;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface RecoveryListener {
    RecoveryListener NOOP = new RecoveryListener() {
        @Override
        public void onRecoveryDone(
            RecoveryState state,
            ShardLongFieldRange timestampMillisFieldRange,
            ShardLongFieldRange eventIngestedMillisFieldRange
        ) {}

        @Override
        public void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy) {}

        @Override
        public void onRecoveryAborted() {}
    };

    /// Called when recovery finishes successfully.
    void onRecoveryDone(
        RecoveryState state,
        ShardLongFieldRange timestampMillisFieldRange,
        ShardLongFieldRange eventIngestedMillisFieldRange
    );

    /// Called when recovery fails with an exception.
    void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy);

    /// Called when recovery has been internally aborted, usually due to shard closure or shard relocation
    void onRecoveryAborted();

    enum FailureStrategy {
        FAIL_SILENT(false),
        FAIL_SEND(true);

        private final boolean notifyMaster;

        FailureStrategy(boolean notifyMaster) {
            this.notifyMaster = notifyMaster;
        }

        public boolean notifyMaster() {
            return notifyMaster;
        }
    }

    static RecoveryListener wrapPreservingContext(RecoveryListener listener, Supplier<ThreadContext.StoredContext> context) {
        return new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                try (ThreadContext.StoredContext ignore = context.get()) {
                    listener.onRecoveryDone(state, timestampMillisFieldRange, eventIngestedMillisFieldRange);
                }
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy) {
                try (ThreadContext.StoredContext ignore = context.get()) {
                    listener.onRecoveryFailure(e, failureStrategy);
                }
            }

            @Override
            public void onRecoveryAborted() {
                try (ThreadContext.StoredContext ignore = context.get()) {
                    listener.onRecoveryAborted();
                }
            }
        };
    }

    /// Returns a listener that delegates all outcomes to the given listener, running `runAfter` after each outcome.
    static RecoveryListener runAfter(RecoveryListener listener, Runnable runAfter) {
        return new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                try {
                    listener.onRecoveryDone(state, timestampMillisFieldRange, eventIngestedMillisFieldRange);
                } finally {
                    runAfter.run();
                }
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy) {
                try {
                    listener.onRecoveryFailure(e, failureStrategy);
                } finally {
                    runAfter.run();
                }
            }

            @Override
            public void onRecoveryAborted() {
                try {
                    listener.onRecoveryAborted();
                } finally {
                    runAfter.run();
                }
            }
        };
    }

    /// Returns a listener that delegates all outcomes to the given listener, running `runBefore` before each outcome.
    static RecoveryListener runBefore(RecoveryListener listener, Runnable runBefore) {
        return new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                try {
                    runBefore.run();
                } finally {
                    listener.onRecoveryDone(state, timestampMillisFieldRange, eventIngestedMillisFieldRange);
                }
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy) {
                try {
                    runBefore.run();
                } finally {
                    listener.onRecoveryFailure(e, failureStrategy);
                }
            }

            @Override
            public void onRecoveryAborted() {
                try {
                    runBefore.run();
                } finally {
                    listener.onRecoveryAborted();
                }
            }
        };
    }

    /// Returns a listener which delegates `onRecoveryFailure` and `onRecoveryAborted` unchanged to the given listener.
    /// Before delegating `onRecoveryDone`, it first runs `beforeDone`.
    static RecoveryListener runBeforeDone(RecoveryListener listener, Runnable beforeDone) {
        return new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                try {
                    beforeDone.run();
                } finally {
                    listener.onRecoveryDone(state, timestampMillisFieldRange, eventIngestedMillisFieldRange);
                }
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy) {
                listener.onRecoveryFailure(e, failureStrategy);
            }

            @Override
            public void onRecoveryAborted() {
                listener.onRecoveryAborted();
            }
        };
    }

    /// Returns a listener which delegates `onRecoveryDone` and `onRecoveryAborted` unchanged to the given listener.
    /// Before delegating `onRecoveryFailure`, it first runs `beforeFailure`.
    static RecoveryListener runBeforeFailure(RecoveryListener listener, Consumer<RecoveryFailedException> beforeFailure) {
        return new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                listener.onRecoveryDone(state, timestampMillisFieldRange, eventIngestedMillisFieldRange);
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy) {
                try {
                    beforeFailure.accept(e);
                } finally {
                    listener.onRecoveryFailure(e, failureStrategy);
                }
            }

            @Override
            public void onRecoveryAborted() {
                listener.onRecoveryAborted();
            }
        };
    }

    /// Returns a listener which (if assertions are enabled) wraps around the given delegate and asserts that it is only called once.
    static RecoveryListener assertOnce(RecoveryListener delegate) {
        if (Assertions.ENABLED) {
            return new RecoveryListener() {
                // if complete, records the stack trace which first completed it
                private final AtomicReference<ElasticsearchException> firstCompletion = new AtomicReference<>();

                private void assertFirstRun() {
                    var previousRun = firstCompletion.compareAndExchange(null, new ElasticsearchException("executed already"));
                    assert previousRun == null
                        // reports the stack traces of both completions
                        : new AssertionError("[" + delegate + "]", previousRun);
                }

                @Override
                public void onRecoveryDone(
                    RecoveryState state,
                    ShardLongFieldRange timestampMillisFieldRange,
                    ShardLongFieldRange eventIngestedMillisFieldRange
                ) {
                    assertFirstRun();
                    try {
                        delegate.onRecoveryDone(state, timestampMillisFieldRange, eventIngestedMillisFieldRange);
                    } catch (Exception e) {
                        assert false : new AssertionError("listener [" + delegate + "] must handle its own exceptions", e);
                        throw e;
                    }
                }

                @Override
                public void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy) {
                    assertFirstRun();
                    try {
                        delegate.onRecoveryFailure(e, failureStrategy);
                    } catch (RuntimeException ex) {
                        if (e != null && ex != e) {
                            ex.addSuppressed(e);
                        }
                        assert false : ex;
                        throw ex;
                    }
                }

                @Override
                public void onRecoveryAborted() {
                    assertFirstRun();
                    try {
                        delegate.onRecoveryAborted();
                    } catch (Exception e) {
                        assert false : new AssertionError("listener [" + delegate + "] must handle its own exceptions", e);
                        throw e;
                    }
                }
            };
        } else {
            return delegate;
        }
    }
}
