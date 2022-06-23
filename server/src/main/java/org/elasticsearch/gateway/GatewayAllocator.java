/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.gateway.AsyncShardFetch.Lister;
import org.elasticsearch.gateway.TransportNodesBatchListGatewayStartedShards.NodeGatewayBatchStartedShard;
import org.elasticsearch.gateway.TransportNodesBatchListGatewayStartedShards.NodeGatewayBatchStartedShards;
import org.elasticsearch.gateway.TransportNodesBatchListGatewayStartedShards.NodesGatewayBatchStartedShards;
import org.elasticsearch.gateway.TransportNodesBatchListGatewayStartedShards.ShardRequestInfo;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodesGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesBatchListShardStoreMetadata;
import org.elasticsearch.indices.store.TransportNodesBatchListShardStoreMetadata.NodeBatchStoreFilesMetadata;
import org.elasticsearch.indices.store.TransportNodesBatchListShardStoreMetadata.NodesBatchStoreFilesMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata.StoreFilesMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.set.Sets.difference;
import static org.elasticsearch.core.Strings.format;

public class GatewayAllocator implements ExistingShardsAllocator {

    public static final String ALLOCATOR_NAME = "gateway_allocator";

    private static final Logger logger = LogManager.getLogger(GatewayAllocator.class);

    private final RerouteService rerouteService;

    private final PrimaryShardAllocator primaryShardAllocator;
    private final ReplicaShardAllocator replicaShardAllocator;

    private final ConcurrentMap<ShardId, AsyncShardFetch<NodeGatewayStartedShards>> asyncFetchStarted = ConcurrentCollections
        .newConcurrentMap();
    private final ConcurrentMap<ShardId, AsyncShardFetch<NodeStoreFilesMetadata>> asyncFetchStore = ConcurrentCollections
        .newConcurrentMap();
    private Set<String> lastSeenEphemeralIds = Collections.emptySet();

    @Inject
    public GatewayAllocator(RerouteService rerouteService, NodeClient client) {
        this.rerouteService = rerouteService;
        this.primaryShardAllocator = new InternalPrimaryShardAllocator(client);
        this.replicaShardAllocator = new InternalReplicaShardAllocator(client);
    }

    @Override
    public void cleanCaches() {
        Releasables.close(asyncFetchStarted.values());
        asyncFetchStarted.clear();
        Releasables.close(asyncFetchStore.values());
        asyncFetchStore.clear();
    }

    // for tests
    protected GatewayAllocator() {
        this.rerouteService = null;
        this.primaryShardAllocator = null;
        this.replicaShardAllocator = null;
    }

    @Override
    public int getNumberOfInFlightFetches() {
        int count = 0;
        for (AsyncShardFetch<NodeGatewayStartedShards> fetch : asyncFetchStarted.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        for (AsyncShardFetch<NodeStoreFilesMetadata> fetch : asyncFetchStore.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        return count;
    }

    @Override
    public void applyStartedShards(final List<ShardRouting> startedShards, final RoutingAllocation allocation) {
        for (ShardRouting startedShard : startedShards) {
            Releasables.close(asyncFetchStarted.remove(startedShard.shardId()));
            Releasables.close(asyncFetchStore.remove(startedShard.shardId()));
        }
    }

    @Override
    public void applyFailedShards(final List<FailedShard> failedShards, final RoutingAllocation allocation) {
        for (FailedShard failedShard : failedShards) {
            Releasables.close(asyncFetchStarted.remove(failedShard.routingEntry().shardId()));
            Releasables.close(asyncFetchStore.remove(failedShard.routingEntry().shardId()));
        }
    }

    @Override
    public void beforeAllocation(final RoutingAllocation allocation) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        ensureAsyncFetchStorePrimaryRecency(allocation);
    }

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {
        assert replicaShardAllocator != null;
        if (allocation.routingNodes().hasInactiveReplicas()) {
            // cancel existing recoveries if we have a better match
            replicaShardAllocator.processExistingRecoveries(allocation);
        }
    }

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        final RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        innerAllocatedUnassigned(allocation, primaryShardAllocator, replicaShardAllocator, shardRouting, unassignedAllocationHandler);
    }

    // allow for testing infra to change shard allocators implementation
    protected static void innerAllocatedUnassigned(
        RoutingAllocation allocation,
        PrimaryShardAllocator primaryShardAllocator,
        ReplicaShardAllocator replicaShardAllocator,
        ShardRouting shardRouting,
        ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        assert shardRouting.unassigned();
        if (shardRouting.primary()) {
            primaryShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
        } else {
            replicaShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
        }
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        assert unassignedShard.unassigned();
        assert routingAllocation.debugDecision();
        if (unassignedShard.primary()) {
            assert primaryShardAllocator != null;
            return primaryShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        } else {
            assert replicaShardAllocator != null;
            return replicaShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        }
    }

    public void flushPendingPrimaryFetchRequests(int batchStepSize) {
        // test case could be null
        if (primaryShardAllocator != null) {
            this.primaryShardAllocator.flushPendingFetchShardRequests(batchStepSize);
        }
    }

    public void flushPendingReplicaFetchRequests(int batchStepSize) {
        // test case could be null
        if (replicaShardAllocator != null) {
            this.replicaShardAllocator.flushPendingFetchShardRequests(batchStepSize);
        }
    }

    public int getPrimaryPendingFetchShardCount() {
        if (primaryShardAllocator == null) {
            return 0;
        }
        return primaryShardAllocator.getPendingFetchShardCount();
    }

    public int getReplicaPendingFetchShardCount() {
        if (replicaShardAllocator == null) {
            return 0;
        }
        return replicaShardAllocator.getPendingFetchShardCount();
    }

    /**
     * Clear the fetched data for the primary to ensure we do not cancel recoveries based on excessively stale data.
     */
    private void ensureAsyncFetchStorePrimaryRecency(RoutingAllocation allocation) {
        DiscoveryNodes nodes = allocation.nodes();
        if (hasNewNodes(nodes)) {
            final Set<String> newEphemeralIds = nodes.getDataNodes()
                .values()
                .stream()
                .map(DiscoveryNode::getEphemeralId)
                .collect(Collectors.toSet());
            // Invalidate the cache if a data node has been added to the cluster. This ensures that we do not cancel a recovery if a node
            // drops out, we fetch the shard data, then some indexing happens and then the node rejoins the cluster again. There are other
            // ways we could decide to cancel a recovery based on stale data (e.g. changing allocation filters or a primary failure) but
            // making the wrong decision here is not catastrophic so we only need to cover the common case.
            logger.trace(
                () -> format(
                    "new nodes %s found, clearing primary async-fetch-store cache",
                    difference(newEphemeralIds, lastSeenEphemeralIds)
                )
            );
            asyncFetchStore.values().forEach(fetch -> clearCacheForPrimary(fetch, allocation));
            // recalc to also (lazily) clear out old nodes.
            this.lastSeenEphemeralIds = newEphemeralIds;
        }
    }

    private static void clearCacheForPrimary(
        AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetch,
        RoutingAllocation allocation
    ) {
        ShardRouting primary = allocation.routingNodes().activePrimary(fetch.shardId);
        if (primary != null) {
            fetch.clearCacheForNode(primary.currentNodeId());
        }
    }

    private boolean hasNewNodes(DiscoveryNodes nodes) {
        for (Map.Entry<String, DiscoveryNode> node : nodes.getDataNodes().entrySet()) {
            if (lastSeenEphemeralIds.contains(node.getValue().getEphemeralId()) == false) {
                return true;
            }
        }
        return false;
    }

    class InternalAsyncFetch<T extends BaseNodeResponse> extends AsyncShardFetch<T> {

        InternalAsyncFetch(
            Logger logger,
            String type,
            ShardId shardId,
            String customDataPath,
            Lister<? extends BaseNodesResponse<T>, T> action
        ) {
            super(logger, type, shardId, customDataPath, action);
        }

        @Override
        protected void reroute(ShardId shardId, String reason) {
            logger.trace("{} scheduling reroute for {}", shardId, reason);
            assert rerouteService != null;
            rerouteService.reroute(
                "async_shard_fetch",
                Priority.HIGH,
                ActionListener.wrap(
                    r -> logger.trace("{} scheduled reroute completed for {}", shardId, reason),
                    e -> logger.debug(() -> format("%s scheduled reroute failed for %s", shardId, reason), e)
                )
            );
        }
    }

    class InternalPrimaryShardAllocator extends PrimaryShardAllocator {

        private final NodeClient client;
        private final AtomicInteger pendingFetchShardCount = new AtomicInteger();
        // node batched shard requests
        private final Map<DiscoveryNode, Map<ShardId, ShardRequestInfo<NodeGatewayStartedShards>>> queuedRequests =
            new ConcurrentHashMap<>();

        InternalPrimaryShardAllocator(NodeClient client) {
            this.client = client;
        }

        @Override
        protected AsyncShardFetch.FetchResult<NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // explicitly type lister, some IDEs (Eclipse) are not able to correctly infer the function type

            AsyncShardFetch<NodeGatewayStartedShards> fetch = asyncFetchStarted.computeIfAbsent(
                shard.shardId(),
                shardId -> allocation.isBatchShardFetchMode()
                    ? new InternalAsyncFetch<>(
                        logger,
                        "batch_shard_started",
                        shardId,
                        IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                        batchLister
                    )
                    : new InternalAsyncFetch<>(
                        logger,
                        "shard_started",
                        shardId,
                        IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                        lister
                    )
            );
            AsyncShardFetch.FetchResult<NodeGatewayStartedShards> shardState = fetch.fetchData(
                allocation.nodes(),
                allocation.getIgnoreNodes(shard.shardId())
            );

            if (shardState.hasData()) {
                shardState.processAllocation(allocation);
            }
            return shardState;
        }

        Lister<BaseNodesResponse<NodeGatewayStartedShards>, NodeGatewayStartedShards> lister = this::listStartedShards;

        private void listStartedShards(
            ShardId shardId,
            String customDataPath,
            DiscoveryNode[] nodes,
            ActionListener<BaseNodesResponse<NodeGatewayStartedShards>> listener
        ) {
            var request = new TransportNodesListGatewayStartedShards.Request(shardId, customDataPath, nodes);
            client.executeLocally(
                TransportNodesListGatewayStartedShards.TYPE,
                request,
                ActionListener.wrap(listener::onResponse, listener::onFailure)
            );
        }

        Lister<BaseNodesResponse<NodeGatewayStartedShards>, NodeGatewayStartedShards> batchLister = this::batchListStartedShards;

        private synchronized void batchListStartedShards(
            ShardId shardId,
            String customDataPath,
            DiscoveryNode[] nodes,
            ActionListener<BaseNodesResponse<NodeGatewayStartedShards>> listener
        ) {
            pendingFetchShardCount.incrementAndGet();
            // group shards by node
            ShardRequestInfo<NodeGatewayStartedShards> shardRequestInfo = new ShardRequestInfo<>(shardId, customDataPath, listener);
            for (DiscoveryNode node : nodes) {
                var nodeLevelRequests = queuedRequests.computeIfAbsent(node, n -> new HashMap<>());
                nodeLevelRequests.put(shardId, shardRequestInfo);
            }
            if (logger.isTraceEnabled()) {
                for (DiscoveryNode node : nodes) {
                    logger.trace(
                        "Queued number of [{}] async fetch shard requests for node [{}]",
                        queuedRequests.get(node).size(),
                        node.getId()
                    );
                }
            }
        };

        @Override
        public synchronized void flushPendingFetchShardRequests(int batchStepSize) {
            if (queuedRequests.isEmpty()) {
                return;
            }
            assert assertLessOrEqualToBatchStepSize(batchStepSize);
            logger.debug("flushing {} primary fetching requests", queuedRequests.size());
            final CountDownLatch nodeRequestLatch = new CountDownLatch(queuedRequests.size());
            for (DiscoveryNode node : queuedRequests.keySet()) {
                Map<ShardId, ShardRequestInfo<NodeGatewayStartedShards>> shardRequests = queuedRequests.get(node);
                Map<ShardId, String> targetShards = new HashMap<>();
                for (ShardId shardId : shardRequests.keySet()) {
                    targetShards.put(shardId, shardRequests.get(shardId).getCustomDataPath());
                }

                assert targetShards.isEmpty() == false;
                logger.debug("Batch sending number of {} primary shard async fetch requests to node {}", targetShards.size(), node.getId());

                // send shards fetch request per node
                final var curNodeRequests = queuedRequests.get(node);
                client.executeLocally(
                    TransportNodesBatchListGatewayStartedShards.TYPE,
                    new TransportNodesBatchListGatewayStartedShards.Request(targetShards, new DiscoveryNode[] { node }),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(NodesGatewayBatchStartedShards nodesBatchStartedShards) {
                            nodeRequestLatch.countDown();
                            if (nodesBatchStartedShards.failures().size() > 0) {
                                // single node, got failed node request then node response must be empty.
                                assert nodesBatchStartedShards.getNodes().size() == 0;
                                for (var request : curNodeRequests.entrySet()) {
                                    request.getValue()
                                        .getListener()
                                        .onResponse(
                                            new NodesGatewayStartedShards(
                                                nodesBatchStartedShards.getClusterName(),
                                                new ArrayList<>(), // empty response
                                                nodesBatchStartedShards.failures()
                                            )
                                        );
                                }
                                return;
                            }

                            assert nodesBatchStartedShards.getNodes().size() == 1;
                            NodeGatewayBatchStartedShards startedShardResponse = nodesBatchStartedShards.getNodes().get(0);
                            List<NodeGatewayBatchStartedShard> startedShards = startedShardResponse.getStartedShards();
                            assert startedShards.size() == curNodeRequests.size();
                            for (NodeGatewayBatchStartedShard shard : startedShards) {
                                // transfer to NodeGatewayStartedShards to bwc.
                                List<NodeGatewayStartedShards> listSingleStartedShards = new ArrayList<>(1);
                                listSingleStartedShards.add(
                                    new NodeGatewayStartedShards(
                                        shard.getNode(),
                                        shard.allocationId(),
                                        shard.primary(),
                                        shard.storeException()
                                    )
                                );

                                NodesGatewayStartedShards newNodesResponse = new NodesGatewayStartedShards(
                                    nodesBatchStartedShards.getClusterName(),
                                    listSingleStartedShards,
                                    nodesBatchStartedShards.failures()
                                );

                                if (logger.isTraceEnabled()) {
                                    logger.trace("got primary {} fetching response from node {}", shard.getShardId(), node.getId());
                                }
                                ShardRequestInfo<NodeGatewayStartedShards> requestInfo = curNodeRequests.get(shard.getShardId());
                                if (requestInfo != null) {
                                    requestInfo.getListener().onResponse(newNodesResponse);
                                } else {
                                    logger.debug("primary shard {} fetching has failed, listener has been cleared", shard.getShardId());
                                }

                                targetShards.remove(shard.getShardId());
                            }

                            // some shards may not respond
                            for (ShardId shard : targetShards.keySet()) {
                                curNodeRequests.get(shard)
                                    .getListener()
                                    .onFailure(
                                        new FailedNodeException(
                                            node.getId(),
                                            "Failed node [" + node.getId() + "]",
                                            new Exception("Failed to fetch " + shard)
                                        )
                                    );
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            nodeRequestLatch.countDown();
                            for (var request : curNodeRequests.entrySet()) {
                                request.getValue().getListener().onFailure(e);
                            }
                        }
                    }
                );
            }

            try {
                nodeRequestLatch.await();
            } catch (InterruptedException e) {
                logger.warn("thread got interrupted while waiting for the node level primary shard fetch response.");
                Thread.currentThread().interrupt();
            } finally {
                queuedRequests.clear();
                pendingFetchShardCount.set(0);
            }
        }

        @Override
        public int getPendingFetchShardCount() {
            return pendingFetchShardCount.get();
        }

        private boolean assertLessOrEqualToBatchStepSize(int batchStepSize) {
            Set<ShardId> shards = new HashSet<>();
            for (var request : queuedRequests.entrySet()) {
                for (var shard : request.getValue().entrySet()) {
                    shards.add(shard.getValue().shardId());
                }
            }
            return shards.size() <= batchStepSize;
        }
    }

    class InternalReplicaShardAllocator extends ReplicaShardAllocator {

        private final NodeClient client;
        private final AtomicInteger pendingFetchShardCount = new AtomicInteger();
        // node batched shard requests
        private final Map<DiscoveryNode, Map<ShardId, ShardRequestInfo<NodeStoreFilesMetadata>>> queuedRequests = new ConcurrentHashMap<>();

        InternalReplicaShardAllocator(NodeClient client) {
            this.client = client;
        }

        @Override
        protected AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // explicitly type lister, some IDEs (Eclipse) are not able to correctly infer the function type
            AsyncShardFetch<NodeStoreFilesMetadata> fetch = asyncFetchStore.computeIfAbsent(
                shard.shardId(),
                shardId -> allocation.isBatchShardFetchMode()
                    ? new InternalAsyncFetch<>(
                        logger,
                        "batch_shard_store",
                        shard.shardId(),
                        IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                        batchLister
                    )
                    : new InternalAsyncFetch<>(
                        logger,
                        "shard_store",
                        shard.shardId(),
                        IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                        lister
                    )
            );
            AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> shardStores = fetch.fetchData(
                allocation.nodes(),
                allocation.getIgnoreNodes(shard.shardId())
            );
            if (shardStores.hasData()) {
                shardStores.processAllocation(allocation);
            }
            return shardStores;
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return asyncFetchStore.get(shard.shardId()) != null;
        }

        Lister<BaseNodesResponse<NodeStoreFilesMetadata>, NodeStoreFilesMetadata> lister = this::listStoreFilesMetadata;

        private void listStoreFilesMetadata(
            ShardId shardId,
            String customDataPath,
            DiscoveryNode[] nodes,
            ActionListener<BaseNodesResponse<NodeStoreFilesMetadata>> listener
        ) {
            var request = new TransportNodesListShardStoreMetadata.Request(shardId, customDataPath, nodes);
            client.executeLocally(
                TransportNodesListShardStoreMetadata.TYPE,
                request,
                ActionListener.wrap(listener::onResponse, listener::onFailure)
            );
        }

        Lister<BaseNodesResponse<NodeStoreFilesMetadata>, NodeStoreFilesMetadata> batchLister = this::batchListStoreFilesMetadata;

        private void batchListStoreFilesMetadata(
            ShardId shardId,
            String customDataPath,
            DiscoveryNode[] nodes,
            ActionListener<BaseNodesResponse<NodeStoreFilesMetadata>> listener
        ) {
            pendingFetchShardCount.incrementAndGet();
            // group shards by node
            ShardRequestInfo<NodeStoreFilesMetadata> shardRequestInfo = new ShardRequestInfo<>(shardId, customDataPath, listener);
            for (DiscoveryNode node : nodes) {
                var nodeLevelRequests = queuedRequests.computeIfAbsent(node, n -> new HashMap<>());
                nodeLevelRequests.put(shardId, shardRequestInfo);
            }
            if (logger.isTraceEnabled()) {
                for (DiscoveryNode node : nodes) {
                    logger.trace(
                        "Queued number of [{}] async list store metadata requests for node [{}]",
                        queuedRequests.get(node).size(),
                        node.getId()
                    );
                }
            }
        };

        @Override
        public void flushPendingFetchShardRequests(int batchStepSize) {
            if (queuedRequests.isEmpty()) {
                return;
            }
            assert assertLessOrEqualToBatchStepSize(batchStepSize);
            logger.debug("flushing {} replica fetching requests", queuedRequests.size());
            final CountDownLatch nodeRequestLatch = new CountDownLatch(queuedRequests.size());
            for (DiscoveryNode node : queuedRequests.keySet()) {
                Map<ShardId, String> targetShards = new HashMap<>();
                var nodeRequest = queuedRequests.get(node);
                for (var shardRequest : nodeRequest.entrySet()) {
                    targetShards.put(shardRequest.getKey(), shardRequest.getValue().getCustomDataPath());
                }

                assert targetShards.isEmpty() == false;
                logger.debug("Batch sending number of {} replica shard async fetch requests to node {}", targetShards.size(), node.getId());

                // send shards fetch request per node
                final var curNodeRequests = queuedRequests.get(node);
                client.executeLocally(
                    TransportNodesBatchListShardStoreMetadata.TYPE,
                    new TransportNodesBatchListShardStoreMetadata.Request(targetShards, new DiscoveryNode[] { node }),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(NodesBatchStoreFilesMetadata nodesBatchStoreFilesMetadata) {
                            nodeRequestLatch.countDown();
                            if (nodesBatchStoreFilesMetadata.failures().size() > 0) {
                                // single node, got failed node request then node response must be empty.
                                assert nodesBatchStoreFilesMetadata.getNodes().size() == 0;
                                for (var request : curNodeRequests.entrySet()) {
                                    request.getValue()
                                        .getListener()
                                        .onResponse(
                                            new NodesStoreFilesMetadata(
                                                nodesBatchStoreFilesMetadata.getClusterName(),
                                                new ArrayList<>(), // empty response
                                                nodesBatchStoreFilesMetadata.failures()
                                            )
                                        );
                                }
                                return;
                            }

                            assert nodesBatchStoreFilesMetadata.getNodes().size() == 1;
                            NodeBatchStoreFilesMetadata storeFilesResponse = nodesBatchStoreFilesMetadata.getNodes().get(0);
                            List<StoreFilesMetadata> storeFiles = storeFilesResponse.storeFilesMetadataList();
                            for (StoreFilesMetadata shard : storeFiles) {
                                // transfer to NodeStoreFilesMetadata to bwc.
                                List<NodeStoreFilesMetadata> listStoreFiles = new ArrayList<>(1);
                                listStoreFiles.add(new NodeStoreFilesMetadata(node, shard));

                                NodesStoreFilesMetadata newNodesResponse = new NodesStoreFilesMetadata(
                                    nodesBatchStoreFilesMetadata.getClusterName(),
                                    listStoreFiles,
                                    nodesBatchStoreFilesMetadata.failures()
                                );

                                if (logger.isTraceEnabled()) {
                                    logger.trace("got replica {} fetching response from node {}", shard.shardId(), node.getId());
                                }

                                ShardRequestInfo<NodeStoreFilesMetadata> requestInfo = curNodeRequests.get(shard.shardId());
                                if (requestInfo != null) {
                                    requestInfo.getListener().onResponse(newNodesResponse);
                                } else {
                                    logger.debug("replica shard {} fetching has failed, listener has been cleared", shard.shardId());
                                }

                                targetShards.remove(shard.shardId());
                            }

                            // some shards don't have copy on this node
                            for (ShardId shard : targetShards.keySet()) {
                                // transfer to NodeStoreFilesMetadata to bwc.
                                List<NodeStoreFilesMetadata> listStoreFiles = new ArrayList<>(1);
                                listStoreFiles.add(new NodeStoreFilesMetadata(node, StoreFilesMetadata.EMPTY));

                                NodesStoreFilesMetadata newNodesResponse = new NodesStoreFilesMetadata(
                                    nodesBatchStoreFilesMetadata.getClusterName(),
                                    listStoreFiles,
                                    nodesBatchStoreFilesMetadata.failures()
                                );

                                ShardRequestInfo<NodeStoreFilesMetadata> requestInfo = curNodeRequests.get(shard);
                                if (requestInfo != null) {
                                    requestInfo.getListener().onResponse(newNodesResponse);
                                } else {
                                    logger.debug("replica shard {} fetching has failed, listener has been cleared", shard);
                                }
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            nodeRequestLatch.countDown();
                            for (var request : curNodeRequests.entrySet()) {
                                request.getValue().getListener().onFailure(e);
                            }
                        }
                    }
                );
            }

            try {
                nodeRequestLatch.await();
            } catch (InterruptedException e) {
                logger.warn("thread got interrupted while waiting for the node level replica shard fetch response.");
                Thread.currentThread().interrupt();
            } finally {
                queuedRequests.clear();
                pendingFetchShardCount.set(0);
            }
        }

        @Override
        public int getPendingFetchShardCount() {
            return pendingFetchShardCount.get();
        }

        private boolean assertLessOrEqualToBatchStepSize(int batchStepSize) {
            Set<ShardId> shards = new HashSet<>();
            for (var request : queuedRequests.entrySet()) {
                for (var shard : request.getValue().entrySet()) {
                    shards.add(shard.getValue().shardId());
                }
            }
            return shards.size() <= batchStepSize;
        }
    }
}
