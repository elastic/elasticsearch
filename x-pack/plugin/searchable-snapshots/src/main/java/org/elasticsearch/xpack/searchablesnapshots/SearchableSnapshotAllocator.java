/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotCacheStoresAction;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotCacheStoresAction.NodeCacheFilesMetadata;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotCacheStoresAction.NodesCacheFilesMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING;

public class SearchableSnapshotAllocator implements ExistingShardsAllocator {

    private final ConcurrentMap<ShardId, AsyncCacheStatusFetch> asyncFetchStore = ConcurrentCollections.newConcurrentMap();

    public static final String ALLOCATOR_NAME = "searchable_snapshot_allocator";

    private final Client client;

    public SearchableSnapshotAllocator(Client client) {
        this.client = client;
    }

    @Override
    public void beforeAllocation(RoutingAllocation allocation) {}

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        if (shardRouting.primary()
            && (shardRouting.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE
                || shardRouting.recoverySource().getType() == RecoverySource.Type.EMPTY_STORE)) {
            // we always force snapshot recovery source to use the snapshot-based recovery process on the node

            final Settings indexSettings = allocation.metadata().index(shardRouting.index()).getSettings();
            final IndexId indexId = new IndexId(
                SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings),
                SNAPSHOT_INDEX_ID_SETTING.get(indexSettings)
            );
            final SnapshotId snapshotId = new SnapshotId(
                SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings),
                SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings)
            );
            final String repository = SNAPSHOT_REPOSITORY_SETTING.get(indexSettings);
            final Snapshot snapshot = new Snapshot(repository, snapshotId);

            shardRouting = unassignedAllocationHandler.updateUnassigned(
                shardRouting.unassignedInfo(),
                new RecoverySource.SnapshotRecoverySource(
                    RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID,
                    snapshot,
                    Version.CURRENT,
                    indexId
                ),
                allocation.changes()
            );
        }

        final AllocateUnassignedDecision allocateUnassignedDecision = decideAllocation(allocation, shardRouting);

        if (allocateUnassignedDecision.isDecisionTaken() && allocateUnassignedDecision.getAllocationDecision() != AllocationDecision.YES) {
            unassignedAllocationHandler.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
        }
    }

    private AllocateUnassignedDecision decideAllocation(RoutingAllocation allocation, ShardRouting shardRouting) {
        assert shardRouting.unassigned();
        assert ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.get(
            allocation.metadata().getIndexSafe(shardRouting.index()).getSettings()
        ).equals(ALLOCATOR_NAME);

        if (shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT
            && allocation.snapshotShardSizeInfo().getShardSize(shardRouting) == null) {
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, null);
        }

        final AsyncShardFetch.FetchResult<NodeCacheFilesMetadata> fetch = fetchData(shardRouting, allocation);
        if (fetch.hasData() == false) {
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, null);
        }

        // let BalancedShardsAllocator take care of allocating this shard
        // TODO: once we have persistent cache, choose a node that has existing data
        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting shardRouting, RoutingAllocation routingAllocation) {
        assert shardRouting.unassigned();
        assert routingAllocation.debugDecision();
        return decideAllocation(routingAllocation, shardRouting);
    }

    @Override
    public void cleanCaches() {
        asyncFetchStore.clear();
    }

    @Override
    public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {
        for (ShardRouting startedShard : startedShards) {
            asyncFetchStore.remove(startedShard.shardId());
        }
    }

    @Override
    public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {
        for (FailedShard failedShard : failedShards) {
            asyncFetchStore.remove(failedShard.getRoutingEntry().shardId());
        }
    }

    @Override
    public int getNumberOfInFlightFetches() {
        int count = 0;
        for (AsyncCacheStatusFetch fetch : asyncFetchStore.values()) {
            count += fetch.numberOfInFlightFetches();
        }
        return count;
    }

    private AsyncShardFetch.FetchResult<NodeCacheFilesMetadata> fetchData(ShardRouting shard, RoutingAllocation allocation) {
        final ShardId shardId = shard.shardId();
        final Settings indexSettings = allocation.metadata().index(shard.index()).getSettings();
        final IndexId indexId = new IndexId(SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings), SNAPSHOT_INDEX_ID_SETTING.get(indexSettings));
        final SnapshotId snapshotId = new SnapshotId(
            SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings),
            SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings)
        );
        final DiscoveryNodes nodes = allocation.nodes();
        final AsyncCacheStatusFetch asyncFetch = asyncFetchStore.computeIfAbsent(shardId, sid -> {
            final AsyncCacheStatusFetch fetch = new AsyncCacheStatusFetch();
            client.execute(
                TransportSearchableSnapshotCacheStoresAction.TYPE,
                new TransportSearchableSnapshotCacheStoresAction.Request(
                    snapshotId,
                    indexId,
                    shardId,
                    nodes.getDataNodes().values().toArray(DiscoveryNode.class)
                ),
                new ActionListener<>() {
                    @Override
                    public void onResponse(NodesCacheFilesMetadata nodesCacheFilesMetadata) {
                        final Map<DiscoveryNode, NodeCacheFilesMetadata> res = new HashMap<>(nodesCacheFilesMetadata.getNodesMap().size());
                        for (Map.Entry<String, NodeCacheFilesMetadata> entry : nodesCacheFilesMetadata.getNodesMap().entrySet()) {
                            res.put(nodes.get(entry.getKey()), entry.getValue());
                        }
                        fetch.data = Collections.unmodifiableMap(res);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // TODO: what now?
                        fetch.data = Collections.emptyMap();
                    }
                }
            );
            return fetch;
        });
        return new AsyncShardFetch.FetchResult<>(shardId, asyncFetch.data(), Collections.emptySet());
    }

    private static final class AsyncCacheStatusFetch {

        private volatile Map<DiscoveryNode, NodeCacheFilesMetadata> data;

        @Nullable
        Map<DiscoveryNode, NodeCacheFilesMetadata> data() {
            return data;
        }

        int numberOfInFlightFetches() {
            return 0;
        }
    }
}
