/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.shared;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.BaseFrozenSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.FrozenCacheInfoNodeAction;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DataTier.TIER_PREFERENCE;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class PartiallyCachedShardAllocationIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // default to no cache: the tests create nodes with a cache configured as needed
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ZERO)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    private MountSearchableSnapshotRequest prepareMountRequest() throws InterruptedException {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepository(fsRepoName, "fs", Settings.builder().put("location", randomRepoPath()));
        assertAcked(prepareCreate(indexName, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));
        populateIndex(indexName, 10);
        ensureGreen(indexName);
        createFullSnapshot(fsRepoName, snapshotName);
        assertAcked(indicesAdmin().prepareDelete(indexName));

        final Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true);

        return new MountSearchableSnapshotRequest(
            indexName,
            fsRepoName,
            snapshotName,
            indexName,
            indexSettingsBuilder.build(),
            Strings.EMPTY_ARRAY,
            true,
            MountSearchableSnapshotRequest.Storage.SHARED_CACHE
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/93868")
    public void testPartialSearchableSnapshotNotAllocatedToNodesWithoutCache() throws Exception {
        final MountSearchableSnapshotRequest req = prepareMountRequest();
        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().clear().setRoutingTable(true).get().getState();
        assertTrue(state.toString(), state.routingTable().index(req.mountedIndexName()).allPrimaryShardsUnassigned());

        final ClusterAllocationExplanation explanation = client().admin()
            .cluster()
            .prepareAllocationExplain()
            .setPrimary(true)
            .setIndex(req.mountedIndexName())
            .setShard(0)
            .get()
            .getExplanation();
        for (NodeAllocationResult nodeDecision : explanation.getShardAllocationDecision().getAllocateDecision().getNodeDecisions()) {
            assertTrue(
                nodeDecision.getNode() + " vs " + Strings.toString(explanation),
                nodeDecision.getCanAllocateDecision()
                    .getDecisions()
                    .stream()
                    .anyMatch(
                        d -> d.getExplanation().contains(SHARED_CACHE_SIZE_SETTING.getKey())
                            && d.getExplanation().contains("shards of partially mounted indices cannot be allocated to this node")
                    )
            );
        }
    }

    public void testPartialSearchableSnapshotAllocatedToNodesWithCache() throws Exception {
        final MountSearchableSnapshotRequest req = prepareMountRequest();

        final List<String> newNodeNames = internalCluster().startDataOnlyNodes(
            between(1, 3),
            Settings.builder()
                .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(randomLongBetween(1, ByteSizeValue.ofMb(10).getBytes())))
                .build()
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(req.mountedIndexName());

        final ClusterState state = client().admin().cluster().prepareState().clear().setNodes(true).setRoutingTable(true).get().getState();
        final Set<String> newNodeIds = newNodeNames.stream().map(n -> state.nodes().resolveNode(n).getId()).collect(Collectors.toSet());
        for (ShardRouting shardRouting : state.routingTable().index(req.mountedIndexName()).shardsWithState(ShardRoutingState.STARTED)) {
            assertThat(state.toString(), newNodeIds, hasItem(shardRouting.currentNodeId()));
        }
    }

    public void testOnlyPartialSearchableSnapshotAllocatedToDedicatedFrozenNodes() throws Exception {

        final MountSearchableSnapshotRequest req = prepareMountRequest();

        final List<String> newNodeNames = internalCluster().startNodes(
            between(1, 3),
            Settings.builder()
                .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(randomLongBetween(1, ByteSizeValue.ofMb(10).getBytes())))
                .put(onlyRole(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE))
                .build()
        );

        createIndex("other-index", Settings.builder().putNull(TIER_PREFERENCE).build());
        ensureGreen("other-index");
        final RoutingNodes routingNodes = client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setRoutingTable(true)
            .setNodes(true)
            .get()
            .getState()
            .getRoutingNodes();
        for (RoutingNode routingNode : routingNodes) {
            if (newNodeNames.contains(routingNode.node().getName())) {
                assertTrue(routingNode + " should be empty in " + routingNodes, routingNode.isEmpty());
            }
        }

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(req.mountedIndexName());

        final ClusterState state = client().admin().cluster().prepareState().clear().setNodes(true).setRoutingTable(true).get().getState();
        final Set<String> newNodeIds = newNodeNames.stream().map(n -> state.nodes().resolveNode(n).getId()).collect(Collectors.toSet());
        for (ShardRouting shardRouting : state.routingTable().index(req.mountedIndexName()).shardsWithState(ShardRoutingState.STARTED)) {
            assertThat(state.toString(), newNodeIds, hasItem(shardRouting.currentNodeId()));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/91800")
    public void testPartialSearchableSnapshotDelaysAllocationUntilNodeCacheStatesKnown() throws Exception {

        updateClusterSettings(
            Settings.builder()
                // forbid rebalancing, we want to check that the initial allocation is balanced
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE)
        );

        final MountSearchableSnapshotRequest req = prepareMountRequest();

        final Map<String, ListenableActionFuture<Void>> cacheInfoBlocks = ConcurrentCollections.newConcurrentMap();
        final Function<String, ListenableActionFuture<Void>> cacheInfoBlockGetter = nodeName -> cacheInfoBlocks.computeIfAbsent(
            nodeName,
            ignored -> new ListenableActionFuture<>()
        );
        // Unblock all the existing nodes
        for (final String nodeName : internalCluster().getNodeNames()) {
            cacheInfoBlockGetter.apply(nodeName).onResponse(null);
        }

        for (final TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            final MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(FrozenCacheInfoNodeAction.NAME)) {
                    cacheInfoBlockGetter.apply(connection.getNode().getName()).addListener(new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void aVoid) {
                            try {
                                connection.sendRequest(requestId, action, request, options);
                            } catch (IOException e) {
                                onFailure(e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new AssertionError("unexpected", e);
                        }
                    });
                } else {
                    connection.sendRequest(requestId, action, request, options);
                }
            });
        }

        final List<String> newNodes = internalCluster().startDataOnlyNodes(
            2,
            Settings.builder()
                .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(randomLongBetween(1, ByteSizeValue.ofMb(10).getBytes())))
                .build()
        );
        final ActionFuture<RestoreSnapshotResponse> responseFuture = client().execute(MountSearchableSnapshotAction.INSTANCE, req);

        assertBusy(() -> {
            try {
                final ClusterAllocationExplanation explanation = client().admin()
                    .cluster()
                    .prepareAllocationExplain()
                    .setPrimary(true)
                    .setIndex(req.mountedIndexName())
                    .setShard(0)
                    .get()
                    .getExplanation();

                assertTrue(Strings.toString(explanation), explanation.getShardAllocationDecision().getAllocateDecision().isDecisionTaken());

                assertThat(
                    Strings.toString(explanation),
                    explanation.getShardAllocationDecision().getAllocateDecision().getAllocationStatus(),
                    equalTo(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA)
                );

            } catch (IndexNotFoundException e) {
                throw new AssertionError("not restored yet", e);
            }
        });

        // Unblock one of the new nodes
        cacheInfoBlockGetter.apply(newNodes.get(1)).onResponse(null);

        // Still won't be allocated
        assertFalse(responseFuture.isDone());
        final ClusterAllocationExplanation explanation = client().admin()
            .cluster()
            .prepareAllocationExplain()
            .setPrimary(true)
            .setIndex(req.mountedIndexName())
            .setShard(0)
            .get()
            .getExplanation();

        assertThat(
            Strings.toString(explanation),
            explanation.getShardAllocationDecision().getAllocateDecision().getAllocationStatus(),
            equalTo(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA)
        );

        // Unblock the other new node, but maybe inject a few errors
        final MockTransportService transportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            newNodes.get(0)
        );
        final Semaphore failurePermits = new Semaphore(between(0, 2));
        transportService.addRequestHandlingBehavior(FrozenCacheInfoNodeAction.NAME, (handler, request, channel, task) -> {
            if (failurePermits.tryAcquire()) {
                channel.sendResponse(new ElasticsearchException("simulated"));
            } else {
                handler.messageReceived(request, channel, task);
            }
        });
        cacheInfoBlockGetter.apply(newNodes.get(0)).onResponse(null);

        final RestoreSnapshotResponse restoreSnapshotResponse = responseFuture.actionGet(10, TimeUnit.SECONDS);
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(req.mountedIndexName());
        assertFalse("should have failed before success", failurePermits.tryAcquire());

        final Map<String, Integer> shardCountsByNodeName = new HashMap<>();
        final ClusterState state = client().admin().cluster().prepareState().clear().setRoutingTable(true).setNodes(true).get().getState();
        for (RoutingNode routingNode : state.getRoutingNodes()) {
            shardCountsByNodeName.put(
                routingNode.node().getName(),
                routingNode.numberOfOwningShardsForIndex(state.routingTable().index(req.mountedIndexName()).getIndex())
            );
        }

        assertThat(
            "balanced across " + newNodes + " in " + state,
            Math.abs(shardCountsByNodeName.get(newNodes.get(0)) - shardCountsByNodeName.get(newNodes.get(1))),
            lessThanOrEqualTo(1)
        );
    }

    @After
    public void cleanUpSettings() {
        updateClusterSettings(Settings.builder().putNull(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey()));
    }
}
