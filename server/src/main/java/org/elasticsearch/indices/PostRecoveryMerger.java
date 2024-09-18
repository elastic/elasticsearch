/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Triggers a check for pending merges when a shard completes recovery.
 */
class PostRecoveryMerger {

    private static final Logger logger = LogManager.getLogger(PostRecoveryMerger.class);

    private static final boolean TRIGGER_MERGE_AFTER_RECOVERY;

    static {
        final var propertyValue = System.getProperty("es.trigger_merge_after_recovery");
        if (propertyValue == null) {
            TRIGGER_MERGE_AFTER_RECOVERY = true;
        } else if ("false".equals(propertyValue)) {
            TRIGGER_MERGE_AFTER_RECOVERY = false;
        } else {
            throw new IllegalStateException(
                "system property [es.trigger_merge_after_recovery] may only be set to [false], but was [" + propertyValue + "]"
            );
        }
    }

    private final ThrottledTaskRunner postRecoveryMergeRunner;
    private final Function<ShardId, IndexShard> shardFunction;

    PostRecoveryMerger(Executor executor, Function<ShardId, IndexShard> shardFunction) {
        this.postRecoveryMergeRunner = new ThrottledTaskRunner(
            getClass().getCanonicalName(),
            // no need to execute these at high concurrency, we just need them to run eventually, so do them one-at-a-time
            1,
            executor
        );
        this.shardFunction = shardFunction;
    }

    PeerRecoveryTargetService.RecoveryListener maybeMergeAfterRecovery(
        ShardId shardId,
        PeerRecoveryTargetService.RecoveryListener recoveryListener
    ) {
        if (TRIGGER_MERGE_AFTER_RECOVERY == false) {
            return recoveryListener;
        }

        logger.trace(Strings.format("wrapping listener for post-recovery merge of [%s]", shardId));

        return new PeerRecoveryTargetService.RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                logger.trace(Strings.format("enqueueing post-recovery merge of [%s]", shardId));
                postRecoveryMergeRunner.enqueueTask(new PostRecoveryMerge(shardId));
                recoveryListener.onRecoveryDone(state, timestampMillisFieldRange, eventIngestedMillisFieldRange);
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                recoveryListener.onRecoveryFailure(e, sendShardFailure);
            }
        };
    }

    class PostRecoveryMerge implements ActionListener<Releasable> {
        private final ShardId shardId;

        PostRecoveryMerge(ShardId shardId) {
            this.shardId = shardId;
        }

        @Override
        public void onResponse(Releasable releasable) {
            logger.trace(Strings.format("attempting post-recovery merge of [%s]", shardId));
            try (releasable) {
                final var indexShard = shardFunction.apply(shardId);
                if (indexShard == null) {
                    return;
                }

                indexShard.triggerPendingMerges();
            } catch (Exception e) {
                logFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            logFailure(e);
        }

        private void logFailure(Exception e) {
            // post-recovery merge is a best-effort thing, failure needs no special handling
            logger.debug(Strings.format("failed to execute post-recovery merge of [%s]", shardId), e);
        }
    }
}
