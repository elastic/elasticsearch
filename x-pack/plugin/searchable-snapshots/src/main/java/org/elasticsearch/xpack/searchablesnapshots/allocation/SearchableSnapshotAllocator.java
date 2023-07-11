/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.gateway.ReplicaShardAllocator;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotCacheStoresAction;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotCacheStoresAction.NodeCacheFilesMetadata;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotCacheStoresAction.NodesCacheFilesMetadata;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheInfoService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.gateway.ReplicaShardAllocator.augmentExplanationsWithStoreInfo;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_UUID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING;

public class SearchableSnapshotAllocator implements ExistingShardsAllocator {

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotAllocator.class);

    private static final ActionListener<Void> REROUTE_LISTENER = new ActionListener<>() {
        @Override
        public void onResponse(Void ignored) {
            logger.trace("reroute succeeded after loading snapshot cache information");
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("reroute failed", e);
        }
    };

    public static final String ALLOCATOR_NAME = "searchable_snapshot_allocator";

    private final ConcurrentMap<ShardId, AsyncCacheStatusFetch> asyncFetchStore = ConcurrentCollections.newConcurrentMap();

    private final Client client;

    private final RerouteService rerouteService;

    private final FrozenCacheInfoService frozenCacheInfoService;

    public SearchableSnapshotAllocator(Client client, RerouteService rerouteService, FrozenCacheInfoService frozenCacheInfoService) {
        this.client = client;
        this.rerouteService = rerouteService;
        this.frozenCacheInfoService = frozenCacheInfoService;
    }

    @Override
    public void beforeAllocation(RoutingAllocation allocation) {
        boolean hasPartialIndices = false;
        for (IndexMetadata indexMetadata : allocation.metadata()) {
            if (indexMetadata.isPartialSearchableSnapshot()) {
                hasPartialIndices = true;
                break;
            }
        }

        if (hasPartialIndices) {
            frozenCacheInfoService.updateNodes(
                client,
                allocation.routingNodes().stream().map(RoutingNode::node).collect(toSet()),
                rerouteService
            );
        } else {
            // clear out any existing entries, which prevents any future retries too
            frozenCacheInfoService.updateNodes(client, Collections.emptySet(), rerouteService);
        }
    }

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        // TODO: cancel and jump to better available allocations?
        if (shardRouting.primary()) {
            final String recoveryUuid = getRecoverySourceRestoreUuid(shardRouting, allocation);
            if (recoveryUuid != null) {

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

                final String repositoryUuid = SNAPSHOT_REPOSITORY_UUID_SETTING.get(indexSettings);
                final String repositoryName;
                if (Strings.hasLength(repositoryUuid) == false) {
                    repositoryName = SNAPSHOT_REPOSITORY_NAME_SETTING.get(indexSettings);
                } else {
                    final RepositoriesMetadata repoMetadata = allocation.metadata().custom(RepositoriesMetadata.TYPE);
                    final List<RepositoryMetadata> repositories = repoMetadata == null ? emptyList() : repoMetadata.repositories();
                    repositoryName = repositories.stream()
                        .filter(r -> repositoryUuid.equals(r.uuid()))
                        .map(RepositoryMetadata::name)
                        .findFirst()
                        .orElse(null);
                }

                if (repositoryName == null) {
                    unassignedAllocationHandler.removeAndIgnore(UnassignedInfo.AllocationStatus.DECIDERS_NO, allocation.changes());
                    return;
                }

                final Snapshot snapshot = new Snapshot(repositoryName, snapshotId);

                final IndexVersion version = shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT
                    ? ((RecoverySource.SnapshotRecoverySource) shardRouting.recoverySource()).version()
                    : IndexVersion.current();

                final RecoverySource.SnapshotRecoverySource recoverySource = new RecoverySource.SnapshotRecoverySource(
                    recoveryUuid,
                    snapshot,
                    version,
                    indexId
                );

                if (shardRouting.recoverySource().equals(recoverySource) == false) {
                    shardRouting = unassignedAllocationHandler.updateUnassigned(
                        shardRouting.unassignedInfo(),
                        recoverySource,
                        allocation.changes()
                    );
                }
            }
        }

        final AllocateUnassignedDecision allocateUnassignedDecision = decideAllocation(allocation, shardRouting);

        if (allocateUnassignedDecision.isDecisionTaken()) {
            if (allocateUnassignedDecision.getAllocationDecision() == AllocationDecision.YES) {
                unassignedAllocationHandler.initialize(
                    allocateUnassignedDecision.getTargetNode().getId(),
                    allocateUnassignedDecision.getAllocationId(),
                    DiskThresholdDecider.getExpectedShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, allocation),
                    allocation.changes()
                );
            } else {
                unassignedAllocationHandler.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
            }
        }
    }

    /**
     * @return the restore UUID to use when adjusting the recovery source of the shard to be a snapshot recovery source from a
     * repository of the correct name. Returns {@code null} if the recovery source should not be adjusted.
     */
    @Nullable
    private static String getRecoverySourceRestoreUuid(ShardRouting shardRouting, RoutingAllocation allocation) {
        switch (shardRouting.recoverySource().getType()) {
            case EXISTING_STORE:
            case EMPTY_STORE:
                // this shard previously failed and/or was force-allocated, so the recovery source must be changed to reflect that it will
                // be recovered from the snapshot again
                return RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID;
            case SNAPSHOT:
                // the recovery source may be correct, or we may be recovering it from a real snapshot (i.e. a backup)

                final RecoverySource.SnapshotRecoverySource recoverySource = (RecoverySource.SnapshotRecoverySource) shardRouting
                    .recoverySource();
                if (recoverySource.restoreUUID().equals(RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID)) {
                    // this shard already has the right recovery ID, but maybe the repository name is now different, so check if it needs
                    // fixing up again
                    return RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID;
                }

                // else we're recovering from a real snapshot, in which case we can only fix up the recovery source once the "real"
                // recovery attempt has completed. It might succeed, but if it doesn't then we replace it with a dummy restore to bypass
                // the RestoreInProgressAllocationDecider

                final RestoreInProgress restoreInProgress = allocation.getClusterState().custom(RestoreInProgress.TYPE);
                if (restoreInProgress == null) {
                    // no ongoing restores, so this shard definitely completed
                    return RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID;
                }

                final RestoreInProgress.Entry entry = restoreInProgress.get(recoverySource.restoreUUID());
                if (entry == null) {
                    // this specific restore is not ongoing, so this shard definitely completed
                    return RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID;
                }

                // else this specific restore is still ongoing, so check whether this shard has completed its attempt yet
                final RestoreInProgress.ShardRestoreStatus shardRestoreStatus = entry.shards().get(shardRouting.shardId());
                if (shardRestoreStatus == null || shardRestoreStatus.state().completed()) {
                    // this shard is not still pending in its specific restore so we can fix up its restore UUID
                    return RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID;
                } else {
                    // this shard is still pending in its specific restore so we must preserve its restore UUID, but we can fix up
                    // the repository name anyway
                    return recoverySource.restoreUUID();
                }
            default:
                return null;
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

        if (SNAPSHOT_PARTIAL_SETTING.get(allocation.metadata().index(shardRouting.index()).getSettings())
            && frozenCacheInfoService.isFetching()) {
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, null);
        }

        final boolean explain = allocation.debugDecision();
        // pre-check if it can be allocated to any node that currently exists, so we won't list the cache sizes for it for nothing
        // TODO: in the following logic, we do not account for existing cache size when handling disk space checks, should and can we
        // reliably do this in a world of concurrent cache evictions or are we ok with the cache size just being a best effort hint
        // here?
        ReplicaShardAllocator.PerNodeAllocationResult result = ReplicaShardAllocator.canBeAllocatedToAtLeastOneNode(
            shardRouting,
            allocation
        );
        Decision allocateDecision = result.decision();
        if (allocateDecision.type() != Decision.Type.YES && (explain == false || asyncFetchStore.get(shardRouting.shardId()) == null)) {
            // only return early if we are not in explain mode, or we are in explain mode but we have not
            // yet attempted to fetch any shard data
            logger.trace("{}: ignoring allocation, can't be allocated on any node", shardRouting);
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.fromDecision(allocateDecision.type()), result.nodes());
        }

        final AsyncShardFetch.FetchResult<NodeCacheFilesMetadata> fetchedCacheData = fetchData(shardRouting, allocation);
        if (fetchedCacheData.hasData() == false) {
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, null);
        }

        final MatchingNodes matchingNodes = findMatchingNodes(shardRouting, allocation, fetchedCacheData, explain);
        assert explain == false || matchingNodes.nodeDecisions != null : "in explain mode, we must have individual node decisions";

        List<NodeAllocationResult> nodeDecisions = augmentExplanationsWithStoreInfo(result.nodes(), matchingNodes.nodeDecisions);
        if (allocateDecision.type() != Decision.Type.YES) {
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.fromDecision(allocateDecision.type()), nodeDecisions);
        } else if (matchingNodes.nodeWithHighestMatch() != null) {
            RoutingNode nodeWithHighestMatch = allocation.routingNodes().node(matchingNodes.nodeWithHighestMatch().getId());
            // we only check on THROTTLE since we checked before on NO
            Decision decision = allocation.deciders().canAllocate(shardRouting, nodeWithHighestMatch, allocation);
            if (decision.type() == Decision.Type.THROTTLE) {
                // TODO: does this make sense? Unlike with the store we could evict the cache concurrently and wait for nothing?
                logger.debug(
                    "[{}][{}]: throttling allocation [{}] to [{}] in order to reuse its unallocated persistent cache",
                    shardRouting.index(),
                    shardRouting.id(),
                    shardRouting,
                    nodeWithHighestMatch.node()
                );
                return AllocateUnassignedDecision.throttle(nodeDecisions);
            } else {
                logger.debug(
                    "[{}][{}]: allocating [{}] to [{}] in order to reuse its persistent cache",
                    shardRouting.index(),
                    shardRouting.id(),
                    shardRouting,
                    nodeWithHighestMatch.node()
                );
                return AllocateUnassignedDecision.yes(nodeWithHighestMatch.node(), null, nodeDecisions, true);
            }
        } else if (isDelayedDueToNodeRestart(allocation, shardRouting)) {
            return ReplicaShardAllocator.delayedDecision(shardRouting, allocation, logger, nodeDecisions);
        }

        // TODO: do we need handling of delayed allocation for leaving replicas here?
        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    private static boolean isDelayedDueToNodeRestart(RoutingAllocation allocation, ShardRouting shardRouting) {
        if (shardRouting.unassignedInfo().isDelayed()) {
            String lastAllocatedNodeId = shardRouting.unassignedInfo().getLastAllocatedNodeId();
            if (lastAllocatedNodeId != null) {
                return allocation.metadata().nodeShutdowns().contains(lastAllocatedNodeId, SingleNodeShutdownMetadata.Type.RESTART);
            }
        }
        return false;
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
        frozenCacheInfoService.clear();
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
            asyncFetchStore.remove(failedShard.routingEntry().shardId());
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

        if (SNAPSHOT_PARTIAL_SETTING.get(indexSettings)) {
            // cached data for partial indices is not persistent, no need to fetch it
            return new AsyncShardFetch.FetchResult<>(shardId, Collections.emptyMap(), Collections.emptySet());
        }

        final SnapshotId snapshotId = new SnapshotId(
            SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings),
            SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings)
        );
        final AsyncCacheStatusFetch asyncFetch = asyncFetchStore.computeIfAbsent(shardId, sid -> new AsyncCacheStatusFetch());
        final DiscoveryNodes nodes = allocation.nodes();
        final DiscoveryNode[] dataNodes = asyncFetch.addFetches(nodes.getDataNodes().values().toArray(DiscoveryNode[]::new));
        if (dataNodes.length > 0) {
            client.execute(
                TransportSearchableSnapshotCacheStoresAction.TYPE,
                new TransportSearchableSnapshotCacheStoresAction.Request(snapshotId, shardId, dataNodes),
                ActionListener.runAfter(new ActionListener<>() {
                    @Override
                    public void onResponse(NodesCacheFilesMetadata nodesCacheFilesMetadata) {
                        final Map<DiscoveryNode, NodeCacheFilesMetadata> res = Maps.newMapWithExpectedSize(
                            nodesCacheFilesMetadata.getNodesMap().size()
                        );
                        for (Map.Entry<String, NodeCacheFilesMetadata> entry : nodesCacheFilesMetadata.getNodesMap().entrySet()) {
                            res.put(nodes.get(entry.getKey()), entry.getValue());
                        }
                        for (FailedNodeException entry : nodesCacheFilesMetadata.failures()) {
                            final DiscoveryNode dataNode = nodes.get(entry.nodeId());
                            logger.warn("Failed fetching cache size from datanode", entry);
                            res.put(dataNode, new NodeCacheFilesMetadata(dataNode, 0L));
                        }
                        asyncFetch.addData(res);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("Failure when trying to fetch existing cache sizes", e);
                        final Map<DiscoveryNode, NodeCacheFilesMetadata> res = Maps.newMapWithExpectedSize(dataNodes.length);
                        for (DiscoveryNode dataNode : dataNodes) {
                            res.put(dataNode, new NodeCacheFilesMetadata(dataNode, 0L));
                        }
                        asyncFetch.addData(res);
                    }
                }, () -> {
                    if (asyncFetch.data() != null) {
                        rerouteService.reroute("async_shard_cache_fetch", Priority.HIGH, REROUTE_LISTENER);
                    }
                })
            );
        }
        return new AsyncShardFetch.FetchResult<>(shardId, asyncFetch.data(), Collections.emptySet());
    }

    private static MatchingNodes findMatchingNodes(
        ShardRouting shard,
        RoutingAllocation allocation,
        AsyncShardFetch.FetchResult<NodeCacheFilesMetadata> data,
        boolean explain
    ) {
        final Map<DiscoveryNode, Long> matchingNodesCacheSizes = new HashMap<>();
        final Map<String, NodeAllocationResult> nodeDecisionsDebug = explain ? new HashMap<>() : null;
        for (Map.Entry<DiscoveryNode, NodeCacheFilesMetadata> nodeStoreEntry : data.getData().entrySet()) {
            DiscoveryNode discoNode = nodeStoreEntry.getKey();
            NodeCacheFilesMetadata nodeCacheFilesMetadata = nodeStoreEntry.getValue();
            // we don't have any existing cached bytes at all
            if (nodeCacheFilesMetadata.bytesCached() == 0L) {
                continue;
            }

            RoutingNode node = allocation.routingNodes().node(discoNode.getId());
            if (node == null) {
                continue;
            }

            // check if we can allocate on the node
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            Long matchingBytes = null;
            if (explain) {
                matchingBytes = nodeCacheFilesMetadata.bytesCached();
                NodeAllocationResult.ShardStoreInfo shardStoreInfo = new NodeAllocationResult.ShardStoreInfo(matchingBytes);
                nodeDecisionsDebug.put(node.nodeId(), new NodeAllocationResult(discoNode, shardStoreInfo, decision));
            }

            if (decision.type() == Decision.Type.NO) {
                continue;
            }

            if (matchingBytes == null) {
                matchingBytes = nodeCacheFilesMetadata.bytesCached();
            }
            matchingNodesCacheSizes.put(discoNode, matchingBytes);
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "{}: node [{}] has [{}/{}] bytes of re-usable cache data",
                    shard,
                    discoNode.getName(),
                    ByteSizeValue.ofBytes(matchingBytes),
                    matchingBytes
                );
            }
        }

        return MatchingNodes.create(matchingNodesCacheSizes, nodeDecisionsDebug);
    }

    private static final class AsyncCacheStatusFetch {

        private final Set<DiscoveryNode> fetchingDataNodes = new HashSet<>();

        private final Map<DiscoveryNode, NodeCacheFilesMetadata> data = new HashMap<>();

        AsyncCacheStatusFetch() {}

        synchronized DiscoveryNode[] addFetches(DiscoveryNode[] nodes) {
            final Collection<DiscoveryNode> nodesToFetch = new ArrayList<>();
            for (DiscoveryNode node : nodes) {
                if (data.containsKey(node) == false && fetchingDataNodes.add(node)) {
                    nodesToFetch.add(node);
                }
            }
            return nodesToFetch.toArray(new DiscoveryNode[0]);
        }

        synchronized void addData(Map<DiscoveryNode, NodeCacheFilesMetadata> newData) {
            data.putAll(newData);
            fetchingDataNodes.removeAll(newData.keySet());
        }

        @Nullable
        synchronized Map<DiscoveryNode, NodeCacheFilesMetadata> data() {
            return fetchingDataNodes.size() > 0 ? null : Map.copyOf(data);
        }

        synchronized int numberOfInFlightFetches() {
            return fetchingDataNodes.size();
        }
    }

    private record MatchingNodes(@Nullable Map<String, NodeAllocationResult> nodeDecisions, @Nullable DiscoveryNode nodeWithHighestMatch) {

        private static MatchingNodes create(
            Map<DiscoveryNode, Long> matchingNodes,
            @Nullable Map<String, NodeAllocationResult> nodeDecisions
        ) {
            return new MatchingNodes(nodeDecisions, getNodeWithHighestMatch(matchingNodes));
        }

        /**
         * Returns the node with the highest number of bytes cached for the shard or {@code null} if no node with any bytes matched exists.
         */
        @Nullable
        private static DiscoveryNode getNodeWithHighestMatch(Map<DiscoveryNode, Long> matchingNodes) {
            return matchingNodes.entrySet()
                .stream()
                .filter(entry -> entry.getValue() > 0L)
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
        }
    }
}
