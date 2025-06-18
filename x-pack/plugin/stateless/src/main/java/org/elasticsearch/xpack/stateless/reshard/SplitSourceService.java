/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.reshard;

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Locale;

public class SplitSourceService {
    private static final Logger logger = LogManager.getLogger(SplitSourceService.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final ObjectStoreService objectStoreService;

    public SplitSourceService(ClusterService clusterService, IndicesService indicesService, ObjectStoreService objectStoreService) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.objectStoreService = objectStoreService;
    }

    public void setupTargetShard(ShardId targetShardId, long sourcePrimaryTerm, long targetPrimaryTerm, ActionListener<Void> listener) {
        Index index = targetShardId.getIndex();

        var indexMetadata = clusterService.state().metadata().projectFor(index).getIndexSafe(index);
        var reshardingMetadata = indexMetadata.getReshardingMetadata();
        assert reshardingMetadata != null && reshardingMetadata.isSplit() : "Unexpected resharding state";
        int sourceShardIndex = reshardingMetadata.getSplit().sourceShard(targetShardId.getId());
        var sourceShardId = new ShardId(index, sourceShardIndex);

        if (reshardingMetadata.getSplit().getSourceShardState(sourceShardIndex) != IndexReshardingState.Split.SourceShardState.SOURCE) {
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Source shard state should be SOURCE but it is [%s]. Failing the request.",
                sourceShardId,
                targetShardId,
                reshardingMetadata.getSplit().getSourceShardState(sourceShardIndex)
            );
            logger.error(message);

            throw new IllegalStateException(message);
        }

        if (reshardingMetadata.getSplit().getTargetShardState(targetShardId.getId()) != IndexReshardingState.Split.TargetShardState.CLONE) {
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Target shard state should be CLONE but it is [%s]. Failing the request.",
                sourceShardId,
                targetShardId,
                reshardingMetadata.getSplit().getTargetShardState(targetShardId.getId())
            );
            logger.error(message);

            throw new IllegalStateException(message);
        }

        long currentSourcePrimaryTerm = indexMetadata.primaryTerm(sourceShardId.getId());
        long currentTargetPrimaryTerm = indexMetadata.primaryTerm(targetShardId.getId());

        if (currentTargetPrimaryTerm > targetPrimaryTerm) {
            // This request is stale, we will handle it when target recovers with new primary term. Fail the target recovery process.
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Target primary term advanced [%s -> %s] before start split request was handled. Failing the request.",
                sourceShardId,
                targetShardId,
                targetPrimaryTerm,
                currentTargetPrimaryTerm
            );
            logger.info(message);

            throw new IllegalStateException(message);
        }
        if (currentSourcePrimaryTerm > sourcePrimaryTerm) {
            // We need to keep the invariant that target primary term is >= source primary term.
            // So if source primary term advanced we need to fail target recovery so that it picks up new primary term.
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Source primary term advanced [%s -> %s] before start split request was handled. Failing the request.",
                sourceShardId,
                targetShardId,
                sourcePrimaryTerm,
                currentSourcePrimaryTerm
            );
            logger.info(message);

            throw new IllegalStateException(message);
        }

        // Defensive check so that some other process (like recovery) won't interfere with resharding.
        var shard = indicesService.indexServiceSafe(index).getShard(sourceShardIndex);
        if (shard.state() != IndexShardState.STARTED) {
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Source shard is not started when processing start split request. Failing the request.",
                sourceShardId,
                targetShardId
            );
            logger.error(message);

            throw new IllegalStateException(message);
        }

        ActionListener.run(listener, l -> {
            objectStoreService.copyShard(sourceShardId, targetShardId, sourcePrimaryTerm);
            l.onResponse(null);
        });
    }
}
