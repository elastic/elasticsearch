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
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.shard.ShardLongFieldRange;

import java.util.concurrent.atomic.AtomicReference;

public interface RecoveryListener {
    RecoveryListener NOOP = new RecoveryListener() {
        @Override
        public void onRecoveryDone(
            RecoveryState state,
            ShardLongFieldRange timestampMillisFieldRange,
            ShardLongFieldRange eventIngestedMillisFieldRange
        ) {}

        @Override
        public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {}

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
    void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure);

    /// Called when recovery has been internally aborted, usually due to shard closure or shard relocation
    void onRecoveryAborted();

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
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                try {
                    listener.onRecoveryFailure(e, sendShardFailure);
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
                public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                    assertFirstRun();
                    try {
                        delegate.onRecoveryFailure(e, sendShardFailure);
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
