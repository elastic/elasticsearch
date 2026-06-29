/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link CacheRestoredAllocationDecider}.
 */
public class CacheRestoredAllocationDeciderTests extends ESAllocationTestCase {

    private static final String SOURCE_NODE_ID = "source-node-id";
    private static final String OTHER_NODE_ID = "other-node-id";
    private static final String TARGET_NODE_ID = "target-node-id";
    private static final String CORRECT_REPLACEMENT_NODE_ID = "correct-replacement-id";

    /**
     * When a node carries the {@code es_cache_restored_from_node} attribute set to the shard's
     * current node ID, the decider should return YES with an explanation referencing the source node.
     */
    public void testYesWithLabelWhenNodeCacheRestoredFromShardCurrentNode() {
        // A shard currently living on SOURCE_NODE_ID.
        final ShardRouting shard = startedShardOnNode(SOURCE_NODE_ID);

        // Target node whose cache was restored from SOURCE_NODE_ID.
        final DiscoveryNode targetNode = DiscoveryNodeUtils.builder(TARGET_NODE_ID)
            .roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE))
            .attributes(Map.of(CacheRestoredAllocationDecider.CACHE_RESTORED_FROM_ATTR, SOURCE_NODE_ID))
            .build();

        final RoutingAllocation allocation = allocationFor(shard, targetNode);
        allocation.debugDecision(true);

        final RoutingNode routingNode = allocation.routingNodes().node(TARGET_NODE_ID);
        final Decision decision = new CacheRestoredAllocationDecider().canAllocate(shard, routingNode, allocation);

        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), containsString(SOURCE_NODE_ID));
    }

    /**
     * Parallel cloning: when the correct replacement (cloned from the shard's source) is present
     * in the cluster alongside a wrong replacement (cloned from a different source), the decider
     * returns NO for the wrong replacement to prevent cross-assignment.
     * Both source nodes are in the cluster so the source-left guard does not fire.
     */
    public void testNoWhenCorrectReplacementExistsInCluster() {
        final ShardRouting shard = startedShardOnNode(SOURCE_NODE_ID);

        // Wrong replacement: its cache was restored from OTHER_NODE_ID, not SOURCE_NODE_ID.
        final DiscoveryNode wrongReplacement = DiscoveryNodeUtils.builder(TARGET_NODE_ID)
            .roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE))
            .attributes(Map.of(CacheRestoredAllocationDecider.CACHE_RESTORED_FROM_ATTR, OTHER_NODE_ID))
            .build();

        // Correct replacement: its cache was restored from SOURCE_NODE_ID.
        final DiscoveryNode correctReplacement = DiscoveryNodeUtils.builder(CORRECT_REPLACEMENT_NODE_ID)
            .roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE))
            .attributes(Map.of(CacheRestoredAllocationDecider.CACHE_RESTORED_FROM_ATTR, SOURCE_NODE_ID))
            .build();

        // The other source node (whose shard is being replaced by wrongReplacement) is still in the cluster.
        final DiscoveryNode otherSourceNode = newNode(OTHER_NODE_ID, Set.of(DiscoveryNodeRole.SEARCH_ROLE));

        final RoutingAllocation allocation = allocationFor(shard, wrongReplacement, correctReplacement, otherSourceNode);
        allocation.debugDecision(true);

        final RoutingNode routingNode = allocation.routingNodes().node(TARGET_NODE_ID);
        final Decision decision = new CacheRestoredAllocationDecider().canAllocate(shard, routingNode, allocation);

        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(decision.getExplanation(), containsString(OTHER_NODE_ID));
        assertThat(decision.getExplanation(), containsString(SOURCE_NODE_ID));
    }

    /**
     * Parallel cloning fallback: when the wrong replacement is in the cluster but the correct
     * replacement is absent (crashed or not yet joined), the decider lifts NO to YES so that
     * shard allocation is never indefinitely stalled.
     */
    public void testYesFallbackWhenCorrectReplacementAbsent() {
        final ShardRouting shard = startedShardOnNode(SOURCE_NODE_ID);

        // Only the wrong replacement is in the cluster.
        final DiscoveryNode wrongReplacement = DiscoveryNodeUtils.builder(TARGET_NODE_ID)
            .roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE))
            .attributes(Map.of(CacheRestoredAllocationDecider.CACHE_RESTORED_FROM_ATTR, OTHER_NODE_ID))
            .build();

        final RoutingAllocation allocation = allocationFor(shard, wrongReplacement);
        allocation.debugDecision(true);

        final RoutingNode routingNode = allocation.routingNodes().node(TARGET_NODE_ID);
        final Decision decision = new CacheRestoredAllocationDecider().canAllocate(shard, routingNode, allocation);

        assertThat(decision.type(), equalTo(Decision.Type.YES));
    }

    /**
     * Once the source node has left the cluster the attribute on the replacement is stale.
     * The decider must return YES unconditionally — even when a "correct" replacement is still
     * present — so that residual NO decisions from the parallel-clone guard are immediately lifted.
     */
    public void testYesWhenSourceNodeHasLeft() {
        final ShardRouting shard = startedShardOnNode(SOURCE_NODE_ID);

        // Wrong replacement: its cache was restored from OTHER_NODE_ID, not SOURCE_NODE_ID.
        final DiscoveryNode wrongReplacement = DiscoveryNodeUtils.builder(TARGET_NODE_ID)
            .roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE))
            .attributes(Map.of(CacheRestoredAllocationDecider.CACHE_RESTORED_FROM_ATTR, OTHER_NODE_ID))
            .build();

        // Correct replacement: its cache was restored from SOURCE_NODE_ID.
        final DiscoveryNode correctReplacement = DiscoveryNodeUtils.builder(CORRECT_REPLACEMENT_NODE_ID)
            .roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE))
            .attributes(Map.of(CacheRestoredAllocationDecider.CACHE_RESTORED_FROM_ATTR, SOURCE_NODE_ID))
            .build();

        // Cluster state without the source node — it has already left.
        final RoutingAllocation allocation = allocationWithoutSource(shard, wrongReplacement, correctReplacement);
        allocation.debugDecision(true);

        final RoutingNode routingNode = allocation.routingNodes().node(TARGET_NODE_ID);
        final Decision decision = new CacheRestoredAllocationDecider().canAllocate(shard, routingNode, allocation);

        // The parallel-clone NO must not be returned once the source has left.
        assertThat(decision.type(), equalTo(Decision.Type.YES));
    }

    /**
     * When the target node has no {@code es_cache_restored_from_node} attribute at all, the
     * decider should return a plain YES without any labelled explanation.
     */
    public void testYesWhenAttributeAbsent() {
        final ShardRouting shard = startedShardOnNode(SOURCE_NODE_ID);

        final DiscoveryNode targetNode = DiscoveryNodeUtils.builder(TARGET_NODE_ID).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build();

        final RoutingAllocation allocation = allocationFor(shard, targetNode);
        allocation.debugDecision(true);

        final RoutingNode routingNode = allocation.routingNodes().node(TARGET_NODE_ID);
        final Decision decision = new CacheRestoredAllocationDecider().canAllocate(shard, routingNode, allocation);

        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision, equalTo(Decision.YES));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Creates a STARTED primary shard whose {@code currentNodeId()} is the given node.
     */
    private static ShardRouting startedShardOnNode(String currentNodeId) {
        final ShardId shardId = new ShardId("test-index", "_na_", 0);
        return TestShardRouting.newShardRouting(shardId, currentNodeId, true, ShardRoutingState.STARTED);
    }

    /**
     * Builds a minimal cluster state containing the shard's source node, the target node under
     * evaluation, and any additional nodes (e.g. other replacement nodes for parallel-clone
     * scenarios), then wraps it in a {@link RoutingAllocation}.
     *
     * @param shard      the shard being considered for placement
     * @param targetNode the node we want to evaluate as an allocation target
     * @param extraNodes additional nodes to include in the cluster (e.g. a correct replacement)
     */
    private static RoutingAllocation allocationFor(ShardRouting shard, DiscoveryNode targetNode, DiscoveryNode... extraNodes) {
        final DiscoveryNode sourceNode = newNode(shard.currentNodeId(), Set.of(DiscoveryNodeRole.SEARCH_ROLE));

        final IndexMetadata indexMetadata = IndexMetadata.builder(shard.getIndexName())
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().add(sourceNode).add(targetNode);
        for (DiscoveryNode extra : extraNodes) {
            nodesBuilder.add(extra);
        }

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(new StatelessShardRoutingRoleStrategy()).addAsNew(indexMetadata).build())
            .build();

        return TestRoutingAllocationFactory.forClusterState(clusterState)
            .allocationDeciders(new AllocationDeciders(Set.of(new CacheRestoredAllocationDecider())))
            .build();
    }

    /**
     * Like {@link #allocationFor} but the source node is intentionally absent from the cluster
     * state, simulating the point after the source has left the cluster.
     *
     * @param shard      the shard being considered for placement (its currentNodeId is the absent source)
     * @param targetNode the node we want to evaluate as an allocation target
     * @param extraNodes additional nodes to include in the cluster
     */
    private static RoutingAllocation allocationWithoutSource(ShardRouting shard, DiscoveryNode targetNode, DiscoveryNode... extraNodes) {
        final IndexMetadata indexMetadata = IndexMetadata.builder(shard.getIndexName())
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().add(targetNode);
        for (DiscoveryNode extra : extraNodes) {
            nodesBuilder.add(extra);
        }

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(new StatelessShardRoutingRoleStrategy()).addAsNew(indexMetadata).build())
            .build();

        return TestRoutingAllocationFactory.forClusterState(clusterState)
            .allocationDeciders(new AllocationDeciders(Set.of(new CacheRestoredAllocationDecider())))
            .build();
    }
}
