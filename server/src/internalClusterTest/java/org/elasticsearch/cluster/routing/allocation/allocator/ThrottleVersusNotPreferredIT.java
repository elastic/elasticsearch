/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ThrottleVersusNotPreferredIT extends ESIntegTestCase {

    private static final Set<String> NOT_PREFERRED_AND_THROTTLED_NODES = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(NotPreferredAndThrottledPlugin.class);
    }

    @After
    public void clearThrottleAndNotPreferredNodes() {
        NOT_PREFERRED_AND_THROTTLED_NODES.clear();
    }

    /**
     * Tests that the Reconciler obeys the THROTTLE decision of a node.
     */
    @TestLogging(
        reason = "track when Reconciler decisions",
        value = "org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceReconciler:TRACE"
    )
    public void testThrottleVsNotPreferredInDesiredBalanceAllocator() {
        final MockLog.SeenEventExpectation expectation = new MockLog.SeenEventExpectation(
            "unable to relocate shard that can no longer remain",
            DesiredBalanceReconciler.class.getCanonicalName(),
            Level.TRACE,
            "Cannot move shard * and cannot remain because of [NO()]"
        );
        runTest(Settings.EMPTY, DesiredBalanceReconciler.class, expectation);
    }

    /**
     * Tests that the non-desired balance allocator obeys the THROTTLE decision of a node.
     */
    @TestLogging(
        reason = "watch for can't move message",
        value = "org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator:TRACE"
    )
    public void testThrottleVsNotPreferredWithNonDesiredBalanceAllocator() {
        final Settings settings = Settings.builder()
            .put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), ClusterModule.BALANCED_ALLOCATOR)
            .build();
        final MockLog.SeenEventExpectation expectation = new MockLog.SeenEventExpectation(
            "unable to relocate shard due to throttling",
            BalancedShardsAllocator.class.getCanonicalName(),
            Level.TRACE,
            "[[*]][0] can't move: [MoveDecision{canMoveDecision=throttled, canRemainDecision=NO(), *}]"
        );
        runTest(settings, BalancedShardsAllocator.class, expectation);
    }

    private void runTest(Settings settings, Class<?> loggerClass, MockLog.SeenEventExpectation movementIsBlockedExpectation) {
        final var sourceNode = internalCluster().startNode(settings);
        final var indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        final var targetNode = internalCluster().startNode(settings);
        final var sourceNodeID = getNodeId(sourceNode);
        final var targetNodeID = getNodeId(targetNode);

        NOT_PREFERRED_AND_THROTTLED_NODES.add(targetNodeID);

        var mapNodeIdsToNames = nodeIdsToNames();
        final var sourceNodeName = mapNodeIdsToNames.get(sourceNodeID);
        final var targetNodeName = mapNodeIdsToNames.get(targetNodeID);
        assertNotNull(sourceNodeName);
        assertNotNull(targetNodeName);

        logger.info("--> Verifying the shard did not move because allocation to the target node (ID: " + targetNodeID + ") is throttled.");
        MockLog.awaitLogger(() -> {
            logger.info(
                "--> Excluding shard assignment to node "
                    + sourceNodeName
                    + "(ID: "
                    + sourceNodeID
                    + ") to force its shard to move to node "
                    + targetNodeName
                    + "(ID: "
                    + targetNodeID
                    + ")"
            );
            updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", sourceNodeName));
        }, loggerClass, movementIsBlockedExpectation);

        // Assert that shard is still on the source node
        final ClusterStateResponse clusterState = safeGet(
            internalCluster().client().admin().cluster().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT))
        );
        assertEquals(
            sourceNodeID,
            clusterState.getState().routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard().currentNodeId()
        );

        logger.info("--> Clearing THROTTLE/NOT_PREFERRED decider response and prodding the balancer (with a reroute request) to try again");
        clearThrottleAndNotPreferredNodes();

        safeGet(
            client().execute(TransportClusterRerouteAction.TYPE, new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
        );

        safeAwait(
            ClusterServiceUtils.addMasterTemporaryStateListener(
                state -> state.routingTable(ProjectId.DEFAULT)
                    .index(indexName)
                    .allShards()
                    .flatMap(IndexShardRoutingTable::allShards)
                    .allMatch(shardRouting -> shardRouting.currentNodeId().equals(targetNodeID) && shardRouting.started())
            )
        );
    }

    public static class NotPreferredAndThrottledPlugin extends Plugin implements ClusterPlugin {
        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {

            return List.of(new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    return NOT_PREFERRED_AND_THROTTLED_NODES.contains(node.nodeId()) ? Decision.NOT_PREFERRED : Decision.YES;
                }
            }, new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    // THROTTLING is not returned in simulation, so only THROTTLE for real moves in the Reconciler.
                    return (NOT_PREFERRED_AND_THROTTLED_NODES.contains(node.nodeId()) && allocation.isSimulating() == false)
                        ? Decision.THROTTLE
                        : Decision.YES;
                }
            });
        }
    }
}
