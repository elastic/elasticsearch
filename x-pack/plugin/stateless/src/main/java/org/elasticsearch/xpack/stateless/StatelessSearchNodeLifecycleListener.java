/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cluster.IndexRemovalReason;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.ClosedShardService;
import org.elasticsearch.xpack.stateless.utils.SearchShardSizeCollector;

import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

class StatelessSearchNodeLifecycleListener implements IndexEventListener {

    private final SearchShardSizeCollector searchShardSizeCollector;
    private final StatelessSharedBlobCacheService sharedBlobCacheService;
    private final IndicesService indicesService;
    private final ClosedShardService closedShardService;
    private final ClusterService clusterService;
    private final BooleanSupplier isNodeShuttingDown;

    StatelessSearchNodeLifecycleListener(
        SearchShardSizeCollector searchShardSizeCollector,
        StatelessSharedBlobCacheService sharedBlobCacheService,
        IndicesService indicesService,
        ClusterService clusterService,
        ClosedShardService closedShardService,
        BooleanSupplier isNodeShuttingDown
    ) {
        this.searchShardSizeCollector = searchShardSizeCollector;
        this.sharedBlobCacheService = sharedBlobCacheService;
        this.indicesService = indicesService;
        this.closedShardService = closedShardService;
        this.clusterService = clusterService;
        this.isNodeShuttingDown = isNodeShuttingDown;
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        searchShardSizeCollector.collectShardSize(indexShard.shardId());
    }

    @Override
    public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
        if (reason == IndexRemovalReason.DELETED) {
            // Evict cache regions of shards of the deleted index
            if (sharedBlobCacheService.isEvictDeletedIndexRegionsEnabled() && isNodeShuttingDown.getAsBoolean() == false) {
                sharedBlobCacheService.forceEvictAsync(k -> k.shardId().getIndex().equals(indexService.index()));
            }
        }
    }

    @Override
    public void onStoreClosed(ShardId shardId) {
        closedShardService.onStoreClose(shardId);

        // Demote cache regions of the closed shard, so they can be more easily evicted
        if (sharedBlobCacheService.isDemoteClosedShardRegionsEnabled() && isNodeShuttingDown.getAsBoolean() == false) {
            final var hasShard = indicesService.hasShardPredicate();
            // Index deletion also ultimately closes the store, but there is no point demoting regions of an index
            // that no longer exists: beforeIndexRemoved above enqueues them for eviction when that is enabled, and
            // otherwise they are left to the regular LFU. We check index existence in the predicate because
            // onStoreClosed can run on the cluster state applier thread, where querying the ClusterService#state()
            // is not allowed.
            final Predicate<ShardId> shouldDemote = id -> isNodeShuttingDown.getAsBoolean() == false
                && clusterService.state().metadata().lookupProject(id.getIndex()).isPresent()
                && hasShard.test(id) == false;
            sharedBlobCacheService.demoteAllAsync(shardId, shouldDemote);
        }
    }
}
