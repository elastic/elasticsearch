/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexVersions;
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

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;

/**
 * Triggers a check for pending merges when a shard completes recovery.
 */
class PostRecoveryMerger {

    private static final Logger logger = LogManager.getLogger(PostRecoveryMerger.class);

    private static final boolean TRIGGER_MERGE_AFTER_RECOVERY;

    static {
        final var propertyValue = System.getProperty("es.trigger_merge_after_recovery_8_515_00_0");
        if (propertyValue == null) {
            TRIGGER_MERGE_AFTER_RECOVERY = true;
        } else if ("false".equals(propertyValue)) {
            TRIGGER_MERGE_AFTER_RECOVERY = false;
        } else {
            throw new IllegalStateException(
                "system property [es.trigger_merge_after_recovery_8_515_00_0] may only be set to [false], but was [" + propertyValue + "]"
            );
        }
    }

    /**
     * Throttled runner to avoid multiple concurrent calls to {@link IndexWriter#maybeMerge()}: we do not need to execute these things
     * especially quickly, as long as they happen eventually, and each such call may involve some IO (reading the soft-deletes doc values to
     * count deleted docs). Note that we're not throttling any actual merges, just the checks to see what merges might be needed. Throttling
     * merges across shards is a separate issue, but normally this mechanism won't trigger any new merges anyway.
     */
    private final ThrottledTaskRunner postRecoveryMergeRunner;

    private final Function<ShardId, IndexShard> shardFunction;
    private final boolean enabled;

    PostRecoveryMerger(Settings settings, Executor executor, Function<ShardId, IndexShard> shardFunction) {
        this.postRecoveryMergeRunner = new ThrottledTaskRunner(getClass().getCanonicalName(), 1, executor);
        this.shardFunction = shardFunction;
        this.enabled =
            // enabled globally ...
            TRIGGER_MERGE_AFTER_RECOVERY
                // ... and we are a node that expects nontrivial amounts of indexing work
                && (DiscoveryNode.hasRole(settings, DATA_HOT_NODE_ROLE)
                    || DiscoveryNode.hasRole(settings, DATA_CONTENT_NODE_ROLE)
                    || DiscoveryNode.hasRole(settings, DATA_ROLE)
                    || DiscoveryNode.hasRole(settings, INDEX_ROLE));
    }

    PeerRecoveryTargetService.RecoveryListener maybeMergeAfterRecovery(
        IndexMetadata indexMetadata,
        ShardRouting shardRouting,
        PeerRecoveryTargetService.RecoveryListener recoveryListener
    ) {
        if (enabled == false) {
            return recoveryListener;
        }

        if (shardRouting.isPromotableToPrimary() == false) {
            return recoveryListener;
        }

        if (indexMetadata.getCreationVersion().before(IndexVersions.MERGE_ON_RECOVERY_VERSION)) {
            return recoveryListener;
        }

        final var shardId = shardRouting.shardId();
        return new PeerRecoveryTargetService.RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
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
            logger.debug(() -> Strings.format("failed to execute post-recovery merge of [%s]", shardId), e);
        }
    }
}
