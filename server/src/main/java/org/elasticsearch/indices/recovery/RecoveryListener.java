/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.index.shard.ShardLongFieldRange;

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
        public void onRecoveryCancelled() {}
    };

    /**
     * Called when recovery finishes successfully.
     */
    void onRecoveryDone(
        RecoveryState state,
        ShardLongFieldRange timestampMillisFieldRange,
        ShardLongFieldRange eventIngestedMillisFieldRange
    );

    /**
     * Called when recovery fails with an exception.
     */
    void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure);

    /**
     * Called when recovery is cancelled, which either means that shard is closing.
     */
    void onRecoveryCancelled();

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
            public void onRecoveryCancelled() {
                try {
                    listener.onRecoveryCancelled();
                } finally {
                    runAfter.run();
                }
            }
        };
    }
}
