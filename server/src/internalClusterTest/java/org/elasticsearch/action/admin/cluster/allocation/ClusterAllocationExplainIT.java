/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.UnassignedInfo.Reason;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.MoveDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for the cluster allocation explanation
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public final class ClusterAllocationExplainIT extends ESIntegTestCase {

    public void testUnassignedPrimaryWithExistingIndex() throws Exception {
        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);

        prepareIndex(1, 0);

        logger.info("--> stopping the node with the primary");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName()));
        ensureStableCluster(1);
        refreshClusterInfo();

        boolean includeYesDecisions = randomBoolean();
        boolean includeDiskInfo = randomBoolean();
        ClusterAllocationExplanation explanation = runExplain(true, includeYesDecisions, includeDiskInfo);

        ShardId shardId = explanation.getShard();
        boolean isPrimary = explanation.isPrimary();
        ShardRoutingState shardState = explanation.getShardState();
        DiscoveryNode currentNode = explanation.getCurrentNode();
        UnassignedInfo unassignedInfo = explanation.getUnassignedInfo();
        ClusterInfo clusterInfo = explanation.getClusterInfo();
        AllocateUnassignedDecision allocateDecision = explanation.getShardAllocationDecision().getAllocateDecision();
        MoveDecision moveDecision = explanation.getShardAllocationDecision().getMoveDecision();

        // verify shard info
        assertEquals("idx", shardId.getIndexName());
        assertEquals(0, shardId.getId());
        assertTrue(isPrimary);

        // verify current node info
        assertNotEquals(ShardRoutingState.STARTED, shardState);
        assertNull(currentNode);

        // verify unassigned info
        assertNotNull(unassignedInfo);
        assertEquals(Reason.NODE_LEFT, unassignedInfo.getReason());
        assertTrue(unassignedInfo.getLastAllocationStatus() == AllocationStatus.FETCHING_SHARD_DATA
                       || unassignedInfo.getLastAllocationStatus() == AllocationStatus.NO_VALID_SHARD_COPY);

        // verify cluster info
        verifyClusterInfo(clusterInfo, includeDiskInfo, 1);

        // verify decision objects
        assertTrue(allocateDecision.isDecisionTaken());
        assertFalse(moveDecision.isDecisionTaken());
        assertTrue(allocateDecision.getAllocationDecision() == AllocationDecision.NO_VALID_SHARD_COPY
                       || allocateDecision.getAllocationDecision() == AllocationDecision.AWAITING_INFO);
        if (allocateDecision.getAllocationDecision() == AllocationDecision.NO_VALID_SHARD_COPY) {
            assertEquals("cannot allocate because a previous copy of the primary shard existed but can no longer be " +
                             "found on the nodes in the cluster", allocateDecision.getExplanation());
        } else {
            assertEquals("cannot allocate because information about existing shard data is still being retrieved from some of the nodes",
                allocateDecision.getExplanation());
        }
        assertNull(allocateDecision.getAllocationId());
        assertNull(allocateDecision.getTargetNode());
        assertEquals(0L, allocateDecision.getConfiguredDelayInMillis());
        assertEquals(0L, allocateDecision.getRemainingDelayInMillis());

        if (allocateDecision.getAllocationDecision() == AllocationDecision.NO_VALID_SHARD_COPY) {
            assertEquals(1, allocateDecision.getNodeDecisions().size());

            // verify JSON output
            try (XContentParser parser = getParser(explanation)) {
                verifyShardInfo(parser, true, includeDiskInfo, ShardRoutingState.UNASSIGNED);
                parser.nextToken();
                assertEquals("can_allocate", parser.currentName());
                parser.nextToken();
                assertEquals(AllocationDecision.NO_VALID_SHARD_COPY.toString(), parser.text());
                parser.nextToken();
                assertEquals("allocate_explanation", parser.currentName());
                parser.nextToken();
                assertEquals("cannot allocate because a previous copy of the primary shard existed but can no longer be found " +
                                 "on the nodes in the cluster", parser.text());
                verifyStaleShardCopyNodeDecisions(parser, 1, Collections.emptySet());
            }
        }
    }

    public void testUnassignedReplicaDelayedAllocation() throws Exception {
        logger.info("--> starting 3 nodes");
        internalCluster().startNodes(3);

        prepareIndex(1, 1);
        logger.info("--> stopping the node with the replica");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNode().getName()));
        ensureStableCluster(2);
        refreshClusterInfo();
        assertBusy(() ->
            // wait till we have passed any pending shard data fetching
            assertEquals(AllocationDecision.ALLOCATION_DELAYED, client().admin().cluster().prepareAllocationExplain()
                .setIndex("idx").setShard(0).setPrimary(false).get().getExplanation()
                .getShardAllocationDecision().getAllocateDecision().getAllocationDecision())
        );

        logger.info("--> observing delayed allocation...");
        boolean includeYesDecisions = randomBoolean();
        boolean includeDiskInfo = randomBoolean();

        ClusterAllocationExplanation explanation = runExplain(false, includeYesDecisions, includeDiskInfo);

        ShardId shardId = explanation.getShard();
        boolean isPrimary = explanation.isPrimary();
        ShardRoutingState shardRoutingState = explanation.getShardState();
        DiscoveryNode currentNode = explanation.getCurrentNode();
        UnassignedInfo unassignedInfo = explanation.getUnassignedInfo();
        ClusterInfo clusterInfo = explanation.getClusterInfo();
        AllocateUnassignedDecision allocateDecision = explanation.getShardAllocationDecision().getAllocateDecision();
        MoveDecision moveDecision = explanation.getShardAllocationDecision().getMoveDecision();

        // verify shard info
        assertEquals("idx", shardId.getIndexName());
        assertEquals(0, shardId.getId());
        assertFalse(isPrimary);

        // verify current node info
        assertNotEquals(ShardRoutingState.STARTED, shardRoutingState);
        assertNull(currentNode);

        // verify unassigned info
        assertNotNull(unassignedInfo);
        assertEquals(Reason.NODE_LEFT, unassignedInfo.getReason());
        assertEquals(AllocationStatus.NO_ATTEMPT, unassignedInfo.getLastAllocationStatus());

        // verify cluster info
        verifyClusterInfo(clusterInfo, includeDiskInfo, 2);

        // verify decision objects
        assertTrue(allocateDecision.isDecisionTaken());
        assertFalse(moveDecision.isDecisionTaken());
        assertEquals(AllocationDecision.ALLOCATION_DELAYED, allocateDecision.getAllocationDecision());
        assertThat(allocateDecision.getExplanation(), startsWith("cannot allocate because the cluster is still waiting"));
        assertThat(allocateDecision.getExplanation(), containsString(
            "despite being allowed to allocate the shard to at least one other node"));
        assertNull(allocateDecision.getAllocationId());
        assertNull(allocateDecision.getTargetNode());
        assertEquals(60000L, allocateDecision.getConfiguredDelayInMillis());
        assertThat(allocateDecision.getRemainingDelayInMillis(), greaterThan(0L));
        assertEquals(2, allocateDecision.getNodeDecisions().size());
        String primaryNodeName = primaryNodeName();
        for (NodeAllocationResult result : allocateDecision.getNodeDecisions()) {
            assertNotNull(result.getNode());
            boolean nodeHoldingPrimary = result.getNode().getName().equals(primaryNodeName);
            if (nodeHoldingPrimary) {
                // shouldn't be able to allocate to the same node as the primary, the same shard decider should say no
                assertEquals(AllocationDecision.NO, result.getNodeDecision());
                assertThat(result.getShardStoreInfo().getMatchingBytes(), greaterThan(0L));
            } else {
                assertEquals(AllocationDecision.YES, result.getNodeDecision());
                assertNull(result.getShardStoreInfo());
            }
            if (includeYesDecisions) {
                assertThat(result.getCanAllocateDecision().getDecisions().size(), greaterThan(1));
            } else {
                // if we are not including YES decisions, then the node holding the primary should have 1 NO decision,
                // the other node should have zero NO decisions
                assertEquals(nodeHoldingPrimary ? 1 : 0, result.getCanAllocateDecision().getDecisions().size());
            }
            for (Decision d : result.getCanAllocateDecision().getDecisions()) {
                if (d.label().equals("same_shard") && nodeHoldingPrimary) {
                    assertEquals(Decision.Type.NO, d.type());
                    assertThat(d.getExplanation(), startsWith("a copy of this shard is already allocated to this node ["));
                } else {
                    assertEquals(Decision.Type.YES, d.type());
                    assertNotNull(d.getExplanation());
                }
            }
        }

        // verify JSON output
        try (XContentParser parser = getParser(explanation)) {
            verifyShardInfo(parser, false, includeDiskInfo, ShardRoutingState.UNASSIGNED);
            parser.nextToken();
            assertEquals("can_allocate", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.ALLOCATION_DELAYED.toString(), parser.text());
            parser.nextToken();
            assertEquals("allocate_explanation", parser.currentName());
            parser.nextToken();
            assertThat(parser.text(), startsWith("cannot allocate because the cluster is still waiting"));
            parser.nextToken();
            assertEquals("configured_delay_in_millis", parser.currentName());
            parser.nextToken();
            assertEquals(60000L, parser.longValue());
            parser.nextToken();
            assertEquals("remaining_delay_in_millis", parser.currentName());
            parser.nextToken();
            assertThat(parser.longValue(), greaterThan(0L));
            Map<String, AllocationDecision> nodes = new HashMap<>();
            nodes.put(primaryNodeName, AllocationDecision.NO);
            String[] currentNodes = internalCluster().getNodeNames();
            nodes.put(currentNodes[0].equals(primaryNodeName) ? currentNodes[1] : currentNodes[0], AllocationDecision.YES);
            verifyNodeDecisions(parser, nodes, includeYesDecisions, true);
            assertEquals(Token.END_OBJECT, parser.nextToken());
        }
    }

    public void testUnassignedReplicaWithPriorCopy() throws Exception {
        logger.info("--> starting 3 nodes");
        List<String> nodes = internalCluster().startNodes(3);

        prepareIndex(1, 1);
        String primaryNodeName = primaryNodeName();
        nodes.remove(primaryNodeName);

        logger.info("--> shutting down all nodes except the one that holds the primary");
        Settings node0DataPathSettings = internalCluster().dataPathSettings(nodes.get(0));
        Settings node1DataPathSettings = internalCluster().dataPathSettings(nodes.get(1));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodes.get(0)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodes.get(1)));
        ensureStableCluster(1);

        logger.info("--> setting allocation filtering to only allow allocation on the currently running node");
        client().admin().indices().prepareUpdateSettings("idx").setSettings(
            Settings.builder().put("index.routing.allocation.include._name", primaryNodeName)).get();

        logger.info("--> restarting the stopped nodes");
        internalCluster().startNode(Settings.builder().put("node.name", nodes.get(0)).put(node0DataPathSettings).build());
        internalCluster().startNode(Settings.builder().put("node.name", nodes.get(1)).put(node1DataPathSettings).build());
        ensureStableCluster(3);

        boolean includeYesDecisions = randomBoolean();
        boolean includeDiskInfo = randomBoolean();
        ClusterAllocationExplanation explanation = runExplain(false, includeYesDecisions, includeDiskInfo);

        ShardId shardId = explanation.getShard();
        boolean isPrimary = explanation.isPrimary();
        ShardRoutingState shardRoutingState = explanation.getShardState();
        DiscoveryNode currentNode = explanation.getCurrentNode();
        UnassignedInfo unassignedInfo = explanation.getUnassignedInfo();
        ClusterInfo clusterInfo = explanation.getClusterInfo();
        AllocateUnassignedDecision allocateDecision = explanation.getShardAllocationDecision().getAllocateDecision();
        MoveDecision moveDecision = explanation.getShardAllocationDecision().getMoveDecision();

        // verify shard info
        assertEquals("idx", shardId.getIndexName());
        assertEquals(0, shardId.getId());
        assertFalse(isPrimary);

        // verify current node info
        assertNotEquals(ShardRoutingState.STARTED, shardRoutingState);
        assertNull(currentNode);

        // verify unassigned info
        assertNotNull(unassignedInfo);
        assertEquals(Reason.NODE_LEFT, unassignedInfo.getReason());
        assertEquals(AllocationStatus.NO_ATTEMPT, unassignedInfo.getLastAllocationStatus());

        // verify cluster info
        verifyClusterInfo(clusterInfo, includeDiskInfo, 3);

        // verify decision objects
        assertTrue(allocateDecision.isDecisionTaken());
        assertFalse(moveDecision.isDecisionTaken());
        AllocationDecision decisionToAllocate = allocateDecision.getAllocationDecision();
        assertTrue(decisionToAllocate == AllocationDecision.AWAITING_INFO || decisionToAllocate == AllocationDecision.NO);
        if (decisionToAllocate == AllocationDecision.AWAITING_INFO) {
            assertEquals("cannot allocate because information about existing shard data is still being retrieved from some of the nodes",
                allocateDecision.getExplanation());
        } else {
            assertEquals("cannot allocate because allocation is not permitted to any of the nodes", allocateDecision.getExplanation());
        }
        assertNull(allocateDecision.getAllocationId());
        assertNull(allocateDecision.getTargetNode());
        assertEquals(0L, allocateDecision.getConfiguredDelayInMillis());
        assertEquals(0L, allocateDecision.getRemainingDelayInMillis());
        assertEquals(3, allocateDecision.getNodeDecisions().size());
        for (NodeAllocationResult result : allocateDecision.getNodeDecisions()) {
            assertNotNull(result.getNode());
            boolean nodeHoldingPrimary = result.getNode().getName().equals(primaryNodeName);
            assertEquals(AllocationDecision.NO, result.getNodeDecision());
            if (includeYesDecisions) {
                assertThat(result.getCanAllocateDecision().getDecisions().size(), greaterThan(1));
            } else {
                assertEquals(1, result.getCanAllocateDecision().getDecisions().size());
            }
            for (Decision d : result.getCanAllocateDecision().getDecisions()) {
                if (d.label().equals("same_shard") && nodeHoldingPrimary) {
                    assertEquals(Decision.Type.NO, d.type());
                    assertThat(d.getExplanation(), startsWith("a copy of this shard is already allocated to this node ["));
                } else if (d.label().equals("filter") && nodeHoldingPrimary == false) {
                    assertEquals(Decision.Type.NO, d.type());
                    assertEquals("node does not match index setting [index.routing.allocation.include] " +
                                     "filters [_name:\"" + primaryNodeName + "\"]", d.getExplanation());
                } else {
                    assertEquals(Decision.Type.YES, d.type());
                    assertNotNull(d.getExplanation());
                }
            }
        }

        // verify JSON output
        try (XContentParser parser = getParser(explanation)) {
            verifyShardInfo(parser, false, includeDiskInfo, ShardRoutingState.UNASSIGNED);
            parser.nextToken();
            assertEquals("can_allocate", parser.currentName());
            parser.nextToken();
            String allocationDecision = parser.text();
            assertTrue(allocationDecision.equals(AllocationDecision.NO.toString())
                           || allocationDecision.equals(AllocationDecision.AWAITING_INFO.toString()));
            parser.nextToken();
            assertEquals("allocate_explanation", parser.currentName());
            parser.nextToken();
            if (allocationDecision.equals("awaiting_info")) {
                assertEquals("cannot allocate because information about existing shard data is still being retrieved " +
                                 "from some of the nodes", parser.text());
            } else {
                assertEquals("cannot allocate because allocation is not permitted to any of the nodes", parser.text());
            }
            Map<String, AllocationDecision> nodeDecisions = new HashMap<>();
            for (String nodeName : internalCluster().getNodeNames()) {
                nodeDecisions.put(nodeName, AllocationDecision.NO);
            }
            verifyNodeDecisions(parser, nodeDecisions, includeYesDecisions, true);
            assertEquals(Token.END_OBJECT, parser.nextToken());
        }
    }

    public void testAllocationFilteringOnIndexCreation() throws Exception {
        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);

        logger.info("--> creating an index with 1 primary, 0 replicas, with allocation filtering so the primary can't be assigned");
        prepareIndex(IndexMetadata.State.OPEN, 1, 0,
            Settings.builder().put("index.routing.allocation.include._name", "non_existent_node").build(),
            ActiveShardCount.NONE);

        boolean includeYesDecisions = randomBoolean();
        boolean includeDiskInfo = randomBoolean();
        ClusterAllocationExplanation explanation = runExplain(true, includeYesDecisions, includeDiskInfo);

        ShardId shardId = explanation.getShard();
        boolean isPrimary = explanation.isPrimary();
        ShardRoutingState shardRoutingState = explanation.getShardState();
        DiscoveryNode currentNode = explanation.getCurrentNode();
        UnassignedInfo unassignedInfo = explanation.getUnassignedInfo();
        ClusterInfo clusterInfo = explanation.getClusterInfo();
        AllocateUnassignedDecision allocateDecision = explanation.getShardAllocationDecision().getAllocateDecision();
        MoveDecision moveDecision = explanation.getShardAllocationDecision().getMoveDecision();

        // verify shard info
        assertEquals("idx", shardId.getIndexName());
        assertEquals(0, shardId.getId());
        assertTrue(isPrimary);

        // verify current node info
        assertNotEquals(ShardRoutingState.STARTED, shardRoutingState);
        assertNull(currentNode);

        // verify unassigned info
        assertNotNull(unassignedInfo);
        assertEquals(Reason.INDEX_CREATED, unassignedInfo.getReason());
        assertEquals(AllocationStatus.DECIDERS_NO, unassignedInfo.getLastAllocationStatus());

        // verify cluster info
        verifyClusterInfo(clusterInfo, includeDiskInfo, 2);

        // verify decision objects
        assertTrue(allocateDecision.isDecisionTaken());
        assertFalse(moveDecision.isDecisionTaken());
        assertEquals(AllocationDecision.NO, allocateDecision.getAllocationDecision());
        assertEquals("cannot allocate because allocation is not permitted to any of the nodes", allocateDecision.getExplanation());
        assertNull(allocateDecision.getAllocationId());
        assertNull(allocateDecision.getTargetNode());
        assertEquals(0L, allocateDecision.getConfiguredDelayInMillis());
        assertEquals(0L, allocateDecision.getRemainingDelayInMillis());
        assertEquals(2, allocateDecision.getNodeDecisions().size());
        for (NodeAllocationResult result : allocateDecision.getNodeDecisions()) {
            assertNotNull(result.getNode());
            assertEquals(AllocationDecision.NO, result.getNodeDecision());
            if (includeYesDecisions) {
                assertThat(result.getCanAllocateDecision().getDecisions().size(), greaterThan(1));
            } else {
                assertEquals(1, result.getCanAllocateDecision().getDecisions().size());
            }
            for (Decision d : result.getCanAllocateDecision().getDecisions()) {
                if (d.label().equals("filter")) {
                    assertEquals(Decision.Type.NO, d.type());
                    assertEquals("node does not match index setting [index.routing.allocation.include] filters " +
                                     "[_name:\"non_existent_node\"]", d.getExplanation());
                }
            }
        }

        // verify JSON output
        try (XContentParser parser = getParser(explanation)) {
            verifyShardInfo(parser, true, includeDiskInfo, ShardRoutingState.UNASSIGNED);
            parser.nextToken();
            assertEquals("can_allocate", parser.currentName());
            parser.nextToken();
            String allocationDecision = parser.text();
            assertTrue(allocationDecision.equals(AllocationDecision.NO.toString())
                           || allocationDecision.equals(AllocationDecision.AWAITING_INFO.toString()));
            parser.nextToken();
            assertEquals("allocate_explanation", parser.currentName());
            parser.nextToken();
            if (allocationDecision.equals("awaiting_info")) {
                assertEquals("cannot allocate because information about existing shard data is still being retrieved " +
                                 "from some of the nodes", parser.text());
            } else {
                assertEquals("cannot allocate because allocation is not permitted to any of the nodes", parser.text());
            }
            Map<String, AllocationDecision> nodeDecisions = new HashMap<>();
            for (String nodeName : internalCluster().getNodeNames()) {
                nodeDecisions.put(nodeName, AllocationDecision.NO);
            }
            verifyNodeDecisions(parser, nodeDecisions, includeYesDecisions, false);
            assertEquals(Token.END_OBJECT, parser.nextToken());
        }
    }

    public void testAllocationFilteringPreventsShardMove() throws Exception {
        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);

        prepareIndex(1, 0);

        logger.info("--> setting up allocation filtering to prevent allocation to both nodes");
        client().admin().indices().prepareUpdateSettings("idx").setSettings(
            Settings.builder().put("index.routing.allocation.include._name", "non_existent_node")).get();

        boolean includeYesDecisions = randomBoolean();
        boolean includeDiskInfo = randomBoolean();
        ClusterAllocationExplanation explanation = runExplain(true, includeYesDecisions, includeDiskInfo);

        ShardId shardId = explanation.getShard();
        boolean isPrimary = explanation.isPrimary();
        ShardRoutingState shardRoutingState = explanation.getShardState();
        DiscoveryNode currentNode = explanation.getCurrentNode();
        UnassignedInfo unassignedInfo = explanation.getUnassignedInfo();
        ClusterInfo clusterInfo = explanation.getClusterInfo();
        AllocateUnassignedDecision allocateDecision = explanation.getShardAllocationDecision().getAllocateDecision();
        MoveDecision moveDecision = explanation.getShardAllocationDecision().getMoveDecision();

        // verify shard info
        assertEquals("idx", shardId.getIndexName());
        assertEquals(0, shardId.getId());
        assertTrue(isPrimary);

        // verify current node info
        assertEquals(ShardRoutingState.STARTED, shardRoutingState);
        assertNotNull(currentNode);

        // verify unassigned info
        assertNull(unassignedInfo);

        // verify cluster info
        verifyClusterInfo(clusterInfo, includeDiskInfo, 2);

        // verify decision object
        assertFalse(allocateDecision.isDecisionTaken());
        assertTrue(moveDecision.isDecisionTaken());
        assertEquals(AllocationDecision.NO, moveDecision.getAllocationDecision());
        assertEquals("cannot move shard to another node, even though it is not allowed to remain on its current node",
            moveDecision.getExplanation());
        assertFalse(moveDecision.canRemain());
        assertFalse(moveDecision.forceMove());
        assertFalse(moveDecision.canRebalanceCluster());
        assertNull(moveDecision.getClusterRebalanceDecision());
        assertNull(moveDecision.getTargetNode());
        assertEquals(0, moveDecision.getCurrentNodeRanking());
        // verifying can remain decision object
        assertNotNull(moveDecision.getCanRemainDecision());
        assertEquals(Decision.Type.NO, moveDecision.getCanRemainDecision().type());
        for (Decision d : moveDecision.getCanRemainDecision().getDecisions()) {
            if (d.label().equals("filter")) {
                assertEquals(Decision.Type.NO, d.type());
                assertEquals("node does not match index setting [index.routing.allocation.include] filters [_name:\"non_existent_node\"]",
                    d.getExplanation());
            } else {
                assertEquals(Decision.Type.YES, d.type());
                assertNotNull(d.getExplanation());
            }
        }
        // verify node decisions
        assertEquals(1, moveDecision.getNodeDecisions().size());
        NodeAllocationResult result = moveDecision.getNodeDecisions().get(0);
        assertNotNull(result.getNode());
        assertEquals(1, result.getWeightRanking());
        assertEquals(AllocationDecision.NO, result.getNodeDecision());
        if (includeYesDecisions) {
            assertThat(result.getCanAllocateDecision().getDecisions().size(), greaterThan(1));
        } else {
            assertEquals(1, result.getCanAllocateDecision().getDecisions().size());
        }
        for (Decision d : result.getCanAllocateDecision().getDecisions()) {
            if (d.label().equals("filter")) {
                assertEquals(Decision.Type.NO, d.type());
                assertEquals("node does not match index setting [index.routing.allocation.include] filters [_name:\"non_existent_node\"]",
                    d.getExplanation());
            } else {
                assertEquals(Decision.Type.YES, d.type());
                assertNotNull(d.getExplanation());
            }
        }

        // verify JSON output
        try (XContentParser parser = getParser(explanation)) {
            verifyShardInfo(parser, true, includeDiskInfo, ShardRoutingState.STARTED);
            parser.nextToken();
            assertEquals("can_remain_on_current_node", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.NO.toString(), parser.text());
            parser.nextToken();
            assertEquals("can_remain_decisions", parser.currentName());
            verifyDeciders(parser, AllocationDecision.NO);
            parser.nextToken();
            assertEquals("can_move_to_other_node", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.NO.toString(), parser.text());
            parser.nextToken();
            assertEquals("move_explanation", parser.currentName());
            parser.nextToken();
            assertEquals("cannot move shard to another node, even though it is not allowed to remain on its current node", parser.text());
            verifyNodeDecisions(parser, allNodeDecisions(AllocationDecision.NO, true), includeYesDecisions, false);
            assertEquals(Token.END_OBJECT, parser.nextToken());
        }
    }

    public void testRebalancingNotAllowed() throws Exception {
        logger.info("--> starting a single node");
        internalCluster().startNode();
        ensureStableCluster(1);

        prepareIndex(5, 0);

        logger.info("--> disabling rebalancing on the index");
        client().admin().indices().prepareUpdateSettings("idx").setSettings(
            Settings.builder().put("index.routing.rebalance.enable", "none")).get();

        logger.info("--> starting another node, with rebalancing disabled, it should get no shards");
        internalCluster().startNode();
        ensureStableCluster(2);

        boolean includeYesDecisions = randomBoolean();
        boolean includeDiskInfo = randomBoolean();
        ClusterAllocationExplanation explanation = runExplain(true, includeYesDecisions, includeDiskInfo);

        ShardId shardId = explanation.getShard();
        boolean isPrimary = explanation.isPrimary();
        ShardRoutingState shardRoutingState = explanation.getShardState();
        DiscoveryNode currentNode = explanation.getCurrentNode();
        UnassignedInfo unassignedInfo = explanation.getUnassignedInfo();
        ClusterInfo clusterInfo = explanation.getClusterInfo();
        AllocateUnassignedDecision allocateDecision = explanation.getShardAllocationDecision().getAllocateDecision();
        MoveDecision moveDecision = explanation.getShardAllocationDecision().getMoveDecision();

        // verify shard info
        assertEquals("idx", shardId.getIndexName());
        assertEquals(0, shardId.getId());
        assertTrue(isPrimary);

        // verify current node info
        assertEquals(ShardRoutingState.STARTED, shardRoutingState);
        assertNotNull(currentNode);

        // verify unassigned info
        assertNull(unassignedInfo);

        // verify cluster info
        verifyClusterInfo(clusterInfo, includeDiskInfo, 2);

        // verify decision object
        assertFalse(allocateDecision.isDecisionTaken());
        assertTrue(moveDecision.isDecisionTaken());
        assertEquals(AllocationDecision.NO, moveDecision.getAllocationDecision());
        assertEquals("rebalancing is not allowed, even though there is at least one node on which the shard can be allocated",
            moveDecision.getExplanation());
        assertTrue(moveDecision.canRemain());
        assertFalse(moveDecision.forceMove());
        assertFalse(moveDecision.canRebalanceCluster());
        assertNotNull(moveDecision.getCanRemainDecision());
        assertNull(moveDecision.getTargetNode());
        assertEquals(2, moveDecision.getCurrentNodeRanking());
        // verifying cluster rebalance decision object
        assertNotNull(moveDecision.getClusterRebalanceDecision());
        assertEquals(Decision.Type.NO, moveDecision.getClusterRebalanceDecision().type());
        for (Decision d : moveDecision.getClusterRebalanceDecision().getDecisions()) {
            if (d.label().equals("enable")) {
                assertEquals(Decision.Type.NO, d.type());
                assertEquals("no rebalancing is allowed due to index setting [index.routing.rebalance.enable=none]",
                    d.getExplanation());
            } else {
                assertEquals(Decision.Type.YES, d.type());
                assertNotNull(d.getExplanation());
            }
        }
        // verify node decisions
        assertEquals(1, moveDecision.getNodeDecisions().size());
        NodeAllocationResult result = moveDecision.getNodeDecisions().get(0);
        assertNotNull(result.getNode());
        assertEquals(1, result.getWeightRanking());
        assertEquals(AllocationDecision.YES, result.getNodeDecision());
        if (includeYesDecisions) {
            assertThat(result.getCanAllocateDecision().getDecisions().size(), greaterThan(0));
        } else {
            assertEquals(0, result.getCanAllocateDecision().getDecisions().size());
        }
        for (Decision d : result.getCanAllocateDecision().getDecisions()) {
            assertEquals(Decision.Type.YES, d.type());
            assertNotNull(d.getExplanation());
        }

        // verify JSON output
        try (XContentParser parser = getParser(explanation)) {
            verifyShardInfo(parser, true, includeDiskInfo, ShardRoutingState.STARTED);
            parser.nextToken();
            assertEquals("can_remain_on_current_node", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.YES.toString(), parser.text());
            parser.nextToken();
            assertEquals("can_rebalance_cluster", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.NO.toString(), parser.text());
            parser.nextToken();
            assertEquals("can_rebalance_cluster_decisions", parser.currentName());
            verifyDeciders(parser, AllocationDecision.NO);
            parser.nextToken();
            assertEquals("can_rebalance_to_other_node", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.NO.toString(), parser.text());
            parser.nextToken();
            assertEquals("rebalance_explanation", parser.currentName());
            parser.nextToken();
            assertEquals("rebalancing is not allowed, even though there is at least one node on which the shard can be allocated",
                parser.text());
            verifyNodeDecisions(parser, allNodeDecisions(AllocationDecision.YES, true), includeYesDecisions, false);
            assertEquals(Token.END_OBJECT, parser.nextToken());
        }
    }

    public void testWorseBalance() throws Exception {
        logger.info("--> starting a single node");
        internalCluster().startNode();
        ensureStableCluster(1);

        prepareIndex(5, 0);

        logger.info("--> setting balancing threshold really high, so it won't be met");
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(
            Settings.builder().put("cluster.routing.allocation.balance.threshold", 1000.0f)).get();

        logger.info("--> starting another node, with the rebalance threshold so high, it should not get any shards");
        internalCluster().startNode();
        ensureStableCluster(2);

        boolean includeYesDecisions = randomBoolean();
        boolean includeDiskInfo = randomBoolean();
        ClusterAllocationExplanation explanation = runExplain(true, includeYesDecisions, includeDiskInfo);

        ShardId shardId = explanation.getShard();
        boolean isPrimary = explanation.isPrimary();
        ShardRoutingState shardRoutingState = explanation.getShardState();
        DiscoveryNode currentNode = explanation.getCurrentNode();
        UnassignedInfo unassignedInfo = explanation.getUnassignedInfo();
        ClusterInfo clusterInfo = explanation.getClusterInfo();
        AllocateUnassignedDecision allocateDecision = explanation.getShardAllocationDecision().getAllocateDecision();
        MoveDecision moveDecision = explanation.getShardAllocationDecision().getMoveDecision();

        // verify shard info
        assertEquals("idx", shardId.getIndexName());
        assertEquals(0, shardId.getId());
        assertTrue(isPrimary);

        // verify current node info
        assertEquals(ShardRoutingState.STARTED, shardRoutingState);
        assertNotNull(currentNode);

        // verify unassigned info
        assertNull(unassignedInfo);

        // verify cluster info
        verifyClusterInfo(clusterInfo, includeDiskInfo, 2);

        // verify decision object
        assertFalse(allocateDecision.isDecisionTaken());
        assertTrue(moveDecision.isDecisionTaken());
        assertEquals(AllocationDecision.NO, moveDecision.getAllocationDecision());
        assertEquals("cannot rebalance as no target node exists that can both allocate this shard and improve the cluster balance",
            moveDecision.getExplanation());
        assertTrue(moveDecision.canRemain());
        assertFalse(moveDecision.forceMove());
        assertTrue(moveDecision.canRebalanceCluster());
        assertNotNull(moveDecision.getCanRemainDecision());
        assertNull(moveDecision.getTargetNode());
        assertEquals(1, moveDecision.getCurrentNodeRanking());
        // verifying cluster rebalance decision object
        assertNotNull(moveDecision.getClusterRebalanceDecision());
        assertEquals(Decision.Type.YES, moveDecision.getClusterRebalanceDecision().type());
        for (Decision d : moveDecision.getClusterRebalanceDecision().getDecisions()) {
            assertEquals(Decision.Type.YES, d.type());
            assertNotNull(d.getExplanation());
        }
        // verify node decisions
        assertEquals(1, moveDecision.getNodeDecisions().size());
        NodeAllocationResult result = moveDecision.getNodeDecisions().get(0);
        assertNotNull(result.getNode());
        assertEquals(1, result.getWeightRanking());
        assertEquals(AllocationDecision.WORSE_BALANCE, result.getNodeDecision());
        if (includeYesDecisions) {
            assertThat(result.getCanAllocateDecision().getDecisions().size(), greaterThan(0));
        } else {
            assertEquals(0, result.getCanAllocateDecision().getDecisions().size());
        }
        for (Decision d : result.getCanAllocateDecision().getDecisions()) {
            assertEquals(Decision.Type.YES, d.type());
            assertNotNull(d.getExplanation());
        }

        // verify JSON output
        try (XContentParser parser = getParser(explanation)) {
            verifyShardInfo(parser, true, includeDiskInfo, ShardRoutingState.STARTED);
            parser.nextToken();
            assertEquals("can_remain_on_current_node", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.YES.toString(), parser.text());
            parser.nextToken();
            assertEquals("can_rebalance_cluster", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.YES.toString(), parser.text());
            parser.nextToken();
            assertEquals("can_rebalance_to_other_node", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.NO.toString(), parser.text());
            parser.nextToken();
            assertEquals("rebalance_explanation", parser.currentName());
            parser.nextToken();
            assertEquals("cannot rebalance as no target node exists that can both allocate this shard and improve the cluster balance",
                parser.text());
            verifyNodeDecisions(parser, allNodeDecisions(AllocationDecision.WORSE_BALANCE, true), includeYesDecisions, false);
            assertEquals(Token.END_OBJECT, parser.nextToken());
        }
    }

    public void testBetterBalanceButCannotAllocate() throws Exception {
        logger.info("--> starting a single node");
        String firstNode = internalCluster().startNode();
        ensureStableCluster(1);

        prepareIndex(5, 0);

        logger.info("--> setting up allocation filtering to only allow allocation to the current node");
        client().admin().indices().prepareUpdateSettings("idx").setSettings(
            Settings.builder().put("index.routing.allocation.include._name", firstNode)).get();

        logger.info("--> starting another node, with filtering not allowing allocation to the new node, it should not get any shards");
        internalCluster().startNode();
        ensureStableCluster(2);

        boolean includeYesDecisions = randomBoolean();
        boolean includeDiskInfo = randomBoolean();
        ClusterAllocationExplanation explanation = runExplain(true, includeYesDecisions, includeDiskInfo);

        ShardId shardId = explanation.getShard();
        boolean isPrimary = explanation.isPrimary();
        ShardRoutingState shardRoutingState = explanation.getShardState();
        DiscoveryNode currentNode = explanation.getCurrentNode();
        UnassignedInfo unassignedInfo = explanation.getUnassignedInfo();
        ClusterInfo clusterInfo = explanation.getClusterInfo();
        AllocateUnassignedDecision allocateDecision = explanation.getShardAllocationDecision().getAllocateDecision();
        MoveDecision moveDecision = explanation.getShardAllocationDecision().getMoveDecision();

        // verify shard info
        assertEquals("idx", shardId.getIndexName());
        assertEquals(0, shardId.getId());
        assertTrue(isPrimary);

        // verify current node info
        assertEquals(ShardRoutingState.STARTED, shardRoutingState);
        assertNotNull(currentNode);

        // verify unassigned info
        assertNull(unassignedInfo);

        // verify cluster info
        verifyClusterInfo(clusterInfo, includeDiskInfo, 2);

        // verify decision object
        assertFalse(allocateDecision.isDecisionTaken());
        assertTrue(moveDecision.isDecisionTaken());
        assertEquals(AllocationDecision.NO, moveDecision.getAllocationDecision());
        assertEquals("cannot rebalance as no target node exists that can both allocate this shard and improve the cluster balance",
            moveDecision.getExplanation());
        assertTrue(moveDecision.canRemain());
        assertFalse(moveDecision.forceMove());
        assertTrue(moveDecision.canRebalanceCluster());
        assertNotNull(moveDecision.getCanRemainDecision());
        assertNull(moveDecision.getTargetNode());
        assertEquals(2, moveDecision.getCurrentNodeRanking());
        // verifying cluster rebalance decision object
        assertNotNull(moveDecision.getClusterRebalanceDecision());
        assertEquals(Decision.Type.YES, moveDecision.getClusterRebalanceDecision().type());
        for (Decision d : moveDecision.getClusterRebalanceDecision().getDecisions()) {
            assertEquals(Decision.Type.YES, d.type());
            assertNotNull(d.getExplanation());
        }
        // verify node decisions
        assertEquals(1, moveDecision.getNodeDecisions().size());
        NodeAllocationResult result = moveDecision.getNodeDecisions().get(0);
        assertNotNull(result.getNode());
        assertEquals(1, result.getWeightRanking());
        assertEquals(AllocationDecision.NO, result.getNodeDecision());
        if (includeYesDecisions) {
            assertThat(result.getCanAllocateDecision().getDecisions().size(), greaterThan(1));
        } else {
            assertEquals(1, result.getCanAllocateDecision().getDecisions().size());
        }
        String primaryNodeName = primaryNodeName();
        for (Decision d : result.getCanAllocateDecision().getDecisions()) {
            if (d.label().equals("filter")) {
                assertEquals(Decision.Type.NO, d.type());
                assertEquals("node does not match index setting [index.routing.allocation.include] filters [_name:\"" +
                                 primaryNodeName + "\"]", d.getExplanation());
            } else {
                assertEquals(Decision.Type.YES, d.type());
                assertNotNull(d.getExplanation());
            }
        }

        // verify JSON output
        try (XContentParser parser = getParser(explanation)) {
            verifyShardInfo(parser, true, includeDiskInfo, ShardRoutingState.STARTED);
            parser.nextToken();
            assertEquals("can_remain_on_current_node", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.YES.toString(), parser.text());
            parser.nextToken();
            assertEquals("can_rebalance_cluster", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.YES.toString(), parser.text());
            parser.nextToken();
            assertEquals("can_rebalance_to_other_node", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.NO.toString(), parser.text());
            parser.nextToken();
            assertEquals("rebalance_explanation", parser.currentName());
            parser.nextToken();
            assertEquals("cannot rebalance as no target node exists that can both allocate this shard and improve the cluster balance",
                parser.text());
            verifyNodeDecisions(parser, allNodeDecisions(AllocationDecision.NO, true), includeYesDecisions, false);
            assertEquals(Token.END_OBJECT, parser.nextToken());
        }
    }

    public void testAssignedReplicaOnSpecificNode() throws Exception {
        logger.info("--> starting 3 nodes");
        List<String> nodes = internalCluster().startNodes(3);

        String excludedNode = nodes.get(randomIntBetween(0, 2));
        prepareIndex(randomIndexState(), 1, 2,
            Settings.builder().put("index.routing.allocation.exclude._name", excludedNode).build(),
            ActiveShardCount.from(2));

        boolean includeYesDecisions = randomBoolean();
        boolean includeDiskInfo = randomBoolean();
        ClusterAllocationExplanation explanation = runExplain(false, replicaNode().getId(), includeYesDecisions, includeDiskInfo);

        ShardId shardId = explanation.getShard();
        boolean isPrimary = explanation.isPrimary();
        ShardRoutingState shardRoutingState = explanation.getShardState();
        DiscoveryNode currentNode = explanation.getCurrentNode();
        UnassignedInfo unassignedInfo = explanation.getUnassignedInfo();
        ClusterInfo clusterInfo = explanation.getClusterInfo();
        AllocateUnassignedDecision allocateDecision = explanation.getShardAllocationDecision().getAllocateDecision();
        MoveDecision moveDecision = explanation.getShardAllocationDecision().getMoveDecision();

        // verify shard info
        assertEquals("idx", shardId.getIndexName());
        assertEquals(0, shardId.getId());
        assertFalse(isPrimary);

        // verify current node info
        assertEquals(ShardRoutingState.STARTED, shardRoutingState);
        assertNotNull(currentNode);
        assertEquals(replicaNode().getName(), currentNode.getName());

        // verify unassigned info
        assertNull(unassignedInfo);

        // verify cluster info
        verifyClusterInfo(clusterInfo, includeDiskInfo, 3);

        // verify decision objects
        assertFalse(allocateDecision.isDecisionTaken());
        assertTrue(moveDecision.isDecisionTaken());
        assertEquals(AllocationDecision.NO, moveDecision.getAllocationDecision());
        assertEquals("rebalancing is not allowed", moveDecision.getExplanation());
        assertTrue(moveDecision.canRemain());
        assertFalse(moveDecision.forceMove());
        assertFalse(moveDecision.canRebalanceCluster());
        assertNotNull(moveDecision.getCanRemainDecision());
        assertNull(moveDecision.getTargetNode());
        // verifying cluster rebalance decision object
        assertNotNull(moveDecision.getClusterRebalanceDecision());
        assertEquals(Decision.Type.NO, moveDecision.getClusterRebalanceDecision().type());
        // verify node decisions
        assertEquals(2, moveDecision.getNodeDecisions().size());
        for (NodeAllocationResult result : moveDecision.getNodeDecisions()) {
            assertNotNull(result.getNode());
            assertEquals(1, result.getWeightRanking());
            assertEquals(AllocationDecision.NO, result.getNodeDecision());
            if (includeYesDecisions) {
                assertThat(result.getCanAllocateDecision().getDecisions().size(), greaterThan(1));
            } else {
                assertEquals(1, result.getCanAllocateDecision().getDecisions().size());
            }
            for (Decision d : result.getCanAllocateDecision().getDecisions()) {
                if (d.type() == Decision.Type.NO) {
                    assertThat(d.label(), is(oneOf("filter", "same_shard")));
                }
                assertNotNull(d.getExplanation());
            }
        }

        // verify JSON output
        try (XContentParser parser = getParser(explanation)) {
            verifyShardInfo(parser, false, includeDiskInfo, ShardRoutingState.STARTED);
            parser.nextToken();
            assertEquals("can_remain_on_current_node", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.YES.toString(), parser.text());
            parser.nextToken();
            assertEquals("can_rebalance_cluster", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.NO.toString(), parser.text());
            parser.nextToken();
            assertEquals("can_rebalance_cluster_decisions", parser.currentName());
            verifyDeciders(parser, AllocationDecision.NO);
            parser.nextToken();
            assertEquals("can_rebalance_to_other_node", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.NO.toString(), parser.text());
            parser.nextToken();
            assertEquals("rebalance_explanation", parser.currentName());
            parser.nextToken();
            assertEquals("rebalancing is not allowed", parser.text());
            verifyNodeDecisions(parser, allNodeDecisions(AllocationDecision.NO, false), includeYesDecisions, false);
            assertEquals(Token.END_OBJECT, parser.nextToken());
        }
    }

    public void testCannotAllocateStaleReplicaExplanation() throws Exception {
        logger.info("--> starting 3 nodes");
        final String masterNode = internalCluster().startNode();
        // start replica node first, so it's path will be used first when we start a node after
        // stopping all of them at end of test.
        final String replicaNode = internalCluster().startNode();
        Settings replicaDataPathSettings = internalCluster().dataPathSettings(replicaNode);
        final String primaryNode = internalCluster().startNode();

        prepareIndex(IndexMetadata.State.OPEN, 1, 1,
            Settings.builder()
                .put("index.routing.allocation.include._name", primaryNode)
                .put("index.routing.allocation.exclude._name", masterNode)
                .build(),
            ActiveShardCount.ONE);

        client().admin().indices().prepareUpdateSettings("idx").setSettings(
            Settings.builder().put("index.routing.allocation.include._name", (String) null)).get();
        ensureGreen();

        assertThat(replicaNode().getName(), equalTo(replicaNode));
        assertThat(primaryNodeName(), equalTo(primaryNode));

        logger.info("--> stop node with the replica shard");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNode));

        final IndexMetadata.State indexState = randomIndexState();
        if (indexState == IndexMetadata.State.OPEN) {
            logger.info("--> index more data, now the replica is stale");
            indexData();
        } else {
            logger.info("--> close the index, now the replica is stale");
            assertAcked(client().admin().indices().prepareClose("idx"));

            final ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth("idx")
                .setTimeout(TimeValue.timeValueSeconds(30))
                .setWaitForActiveShards(ActiveShardCount.ONE)
                .setWaitForNoInitializingShards(true)
                .setWaitForEvents(Priority.LANGUID)
                .get();
            assertThat(clusterHealthResponse.getStatus().value(), lessThanOrEqualTo(ClusterHealthStatus.YELLOW.value()));
        }

        logger.info("--> stop the node with the primary");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));

        logger.info("--> restart the node with the stale replica");
        String restartedNode = internalCluster().startDataOnlyNode(replicaDataPathSettings);
        ensureClusterSizeConsistency(); // wait for the master to finish processing join.

        // wait until the system has fetched shard data and we know there is no valid shard copy
        assertBusy(() -> {
            ClusterAllocationExplanation explanation = client().admin().cluster().prepareAllocationExplain()
                .setIndex("idx").setShard(0).setPrimary(true).get().getExplanation();
            assertTrue(explanation.getShardAllocationDecision().getAllocateDecision().isDecisionTaken());
            assertEquals(AllocationDecision.NO_VALID_SHARD_COPY,
                explanation.getShardAllocationDecision().getAllocateDecision().getAllocationDecision());
        });
        boolean includeYesDecisions = randomBoolean();
        boolean includeDiskInfo = randomBoolean();
        ClusterAllocationExplanation explanation = runExplain(true, includeYesDecisions, includeDiskInfo);

        ShardId shardId = explanation.getShard();
        boolean isPrimary = explanation.isPrimary();
        ShardRoutingState shardRoutingState = explanation.getShardState();
        DiscoveryNode currentNode = explanation.getCurrentNode();
        UnassignedInfo unassignedInfo = explanation.getUnassignedInfo();
        AllocateUnassignedDecision allocateDecision = explanation.getShardAllocationDecision().getAllocateDecision();
        MoveDecision moveDecision = explanation.getShardAllocationDecision().getMoveDecision();

        // verify shard info
        assertEquals("idx", shardId.getIndexName());
        assertEquals(0, shardId.getId());
        assertTrue(isPrimary);

        // verify current node info
        assertEquals(ShardRoutingState.UNASSIGNED, shardRoutingState);
        assertNull(currentNode);
        // verify unassigned info
        assertNotNull(unassignedInfo);

        // verify decision object
        assertTrue(allocateDecision.isDecisionTaken());
        assertFalse(moveDecision.isDecisionTaken());
        assertEquals(AllocationDecision.NO_VALID_SHARD_COPY, allocateDecision.getAllocationDecision());
        assertEquals(2, allocateDecision.getNodeDecisions().size());
        for (NodeAllocationResult nodeAllocationResult : allocateDecision.getNodeDecisions()) {
            if (nodeAllocationResult.getNode().getName().equals(restartedNode)) {
                assertNotNull(nodeAllocationResult.getShardStoreInfo());
                assertNotNull(nodeAllocationResult.getShardStoreInfo().getAllocationId());
                assertFalse(nodeAllocationResult.getShardStoreInfo().isInSync());
                assertNull(nodeAllocationResult.getShardStoreInfo().getStoreException());
            } else {
                assertNotNull(nodeAllocationResult.getShardStoreInfo());
                assertNull(nodeAllocationResult.getShardStoreInfo().getAllocationId());
                assertFalse(nodeAllocationResult.getShardStoreInfo().isInSync());
                assertNull(nodeAllocationResult.getShardStoreInfo().getStoreException());
            }
        }

        // verify JSON output
        try (XContentParser parser = getParser(explanation)) {
            verifyShardInfo(parser, true, includeDiskInfo, ShardRoutingState.UNASSIGNED);
            parser.nextToken();
            assertEquals("can_allocate", parser.currentName());
            parser.nextToken();
            assertEquals(AllocationDecision.NO_VALID_SHARD_COPY.toString(), parser.text());
            parser.nextToken();
            assertEquals("allocate_explanation", parser.currentName());
            parser.nextToken();
            assertEquals("cannot allocate because all found copies of the shard are either stale or corrupt", parser.text());
            verifyStaleShardCopyNodeDecisions(parser, 2, Collections.singleton(restartedNode));
        }
    }

    private void verifyClusterInfo(ClusterInfo clusterInfo, boolean includeDiskInfo, int numNodes) {
        if (includeDiskInfo) {
            assertThat(clusterInfo.getNodeMostAvailableDiskUsages().size(), greaterThanOrEqualTo(0));
            assertThat(clusterInfo.getNodeLeastAvailableDiskUsages().size(), greaterThanOrEqualTo(0));
            assertThat(clusterInfo.getNodeMostAvailableDiskUsages().size(), lessThanOrEqualTo(numNodes));
            assertThat(clusterInfo.getNodeLeastAvailableDiskUsages().size(), lessThanOrEqualTo(numNodes));
        } else {
            assertNull(clusterInfo);
        }
    }

    private ClusterAllocationExplanation runExplain(boolean primary, boolean includeYesDecisions, boolean includeDiskInfo)
        throws Exception {

        return runExplain(primary, null, includeYesDecisions, includeDiskInfo);
    }

    private ClusterAllocationExplanation runExplain(boolean primary, String nodeId, boolean includeYesDecisions, boolean includeDiskInfo)
        throws Exception {

        ClusterAllocationExplanation explanation = client().admin().cluster().prepareAllocationExplain()
            .setIndex("idx").setShard(0).setPrimary(primary)
            .setIncludeYesDecisions(includeYesDecisions)
            .setIncludeDiskInfo(includeDiskInfo)
            .setCurrentNode(nodeId)
            .get().getExplanation();
        if (logger.isDebugEnabled()) {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.prettyPrint();
            builder.humanReadable(true);
            logger.debug("--> explain json output: \n{}", Strings.toString(explanation.toXContent(builder, ToXContent.EMPTY_PARAMS)));
        }
        return explanation;
    }

    private void prepareIndex(final int numPrimaries, final int numReplicas) {
        prepareIndex(randomIndexState(), numPrimaries, numReplicas, Settings.EMPTY, ActiveShardCount.ALL);
    }

    private void prepareIndex(final IndexMetadata.State state, final int numPrimaries, final int numReplicas,
                              final Settings settings, final ActiveShardCount activeShardCount) {

        logger.info("--> creating a {} index with {} primary, {} replicas", state, numPrimaries, numReplicas);
        assertAcked(client().admin().indices().prepareCreate("idx")
            .setSettings(Settings.builder()
                             .put("index.number_of_shards", numPrimaries)
                             .put("index.number_of_replicas", numReplicas)
                             .put(settings))
            .setWaitForActiveShards(activeShardCount)
            .get());

        if (activeShardCount != ActiveShardCount.NONE) {
            indexData();
        }
        if (state == IndexMetadata.State.CLOSE) {
            assertAcked(client().admin().indices().prepareClose("idx"));

            final ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth("idx")
                .setTimeout(TimeValue.timeValueSeconds(30))
                .setWaitForActiveShards(activeShardCount)
                .setWaitForEvents(Priority.LANGUID)
                .get();
            assertThat(clusterHealthResponse.getStatus().value(), lessThanOrEqualTo(ClusterHealthStatus.YELLOW.value()));
        }
    }

    private static IndexMetadata.State randomIndexState() {
        return randomFrom(IndexMetadata.State.values());
    }

    private void indexData() {
        for (int i = 0; i < 10; i++) {
            index("idx", Integer.toString(i), Collections.singletonMap("f1", Integer.toString(i)));
        }
        flushAndRefresh("idx");
    }

    private String primaryNodeName() {
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String nodeId = clusterState.getRoutingTable().index("idx").shard(0).primaryShard().currentNodeId();
        return clusterState.getRoutingNodes().node(nodeId).node().getName();
    }

    private DiscoveryNode replicaNode() {
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String nodeId = clusterState.getRoutingTable().index("idx").shard(0).replicaShards().get(0).currentNodeId();
        return clusterState.getRoutingNodes().node(nodeId).node();
    }

    private XContentParser getParser(ClusterAllocationExplanation explanation) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        return createParser(explanation.toXContent(builder, ToXContent.EMPTY_PARAMS));
    }

    private void verifyShardInfo(XContentParser parser, boolean primary, boolean includeDiskInfo, ShardRoutingState state)
        throws IOException {

        parser.nextToken();
        assertEquals(Token.START_OBJECT, parser.currentToken());
        parser.nextToken();
        assertEquals("index", parser.currentName());
        parser.nextToken();
        assertEquals("idx", parser.text());
        parser.nextToken();
        assertEquals("shard", parser.currentName());
        parser.nextToken();
        assertEquals(0, parser.intValue());
        parser.nextToken();
        assertEquals("primary", parser.currentName());
        parser.nextToken();
        assertEquals(primary, parser.booleanValue());
        parser.nextToken();
        assertEquals("current_state", parser.currentName());
        parser.nextToken();
        assertEquals(state.toString().toLowerCase(Locale.ROOT), parser.text());
        if (state == ShardRoutingState.UNASSIGNED) {
            parser.nextToken();
            assertEquals("unassigned_info", parser.currentName());
            assertEquals(Token.START_OBJECT, parser.nextToken());
            Token token;
            while ((token = parser.nextToken()) != Token.END_OBJECT) { // until we reach end of unassigned_info
                if (token == XContentParser.Token.FIELD_NAME) {
                    assertNotEquals("delayed", parser.currentName()); // we should never display "delayed" from unassigned info
                    if (parser.currentName().equals("last_allocation_status")) {
                        parser.nextToken();
                        assertThat(parser.text(), is(oneOf(AllocationDecision.NO.toString(),
                            AllocationDecision.NO_VALID_SHARD_COPY.toString(),
                            AllocationDecision.AWAITING_INFO.toString(),
                            AllocationDecision.NO_ATTEMPT.toString())));
                    }
                }
            }
        } else {
            assertEquals(ShardRoutingState.STARTED, state);
            parser.nextToken();
            assertEquals("current_node", parser.currentName());
            assertEquals(Token.START_OBJECT, parser.nextToken());
            Token token;
            while ((token = parser.nextToken()) != Token.END_OBJECT) { // until we reach end of current_node
                if (token == Token.FIELD_NAME) {
                    assertTrue(parser.currentName().equals("id")
                                   || parser.currentName().equals("name")
                                   || parser.currentName().equals("transport_address")
                                   || parser.currentName().equals("weight_ranking"));
                } else {
                    assertTrue(token.isValue());
                    assertNotNull(parser.text());
                }
            }
        }
        if (includeDiskInfo) {
            // disk info is included, just verify the object is there
            parser.nextToken();
            assertEquals("cluster_info", parser.currentName());
            assertEquals(Token.START_OBJECT, parser.nextToken());
            int numObjects = 1;
            while (numObjects > 0) {
                Token token = parser.nextToken();
                if (token == Token.START_OBJECT) {
                    ++numObjects;
                } else if (token == Token.END_OBJECT) {
                    --numObjects;
                }
            }
        }
    }

    private void verifyStaleShardCopyNodeDecisions(XContentParser parser, int numNodes, Set<String> foundStores) throws IOException {
        parser.nextToken();
        assertEquals("node_allocation_decisions", parser.currentName());
        assertEquals(Token.START_ARRAY, parser.nextToken());
        for (int i = 0; i < numNodes; i++) {
            String nodeName = verifyNodeDecisionPrologue(parser);
            assertEquals(AllocationDecision.NO.toString(), parser.text());
            parser.nextToken();
            assertEquals("store", parser.currentName());
            assertEquals(Token.START_OBJECT, parser.nextToken());
            parser.nextToken();
            if (foundStores.contains(nodeName)) {
                // shard data was found on the node, but it is stale
                assertEquals("in_sync", parser.currentName());
                parser.nextToken();
                assertFalse(parser.booleanValue());
                parser.nextToken();
                assertEquals("allocation_id", parser.currentName());
                parser.nextToken();
                assertNotNull(parser.text());
            } else {
                // no shard data was found on the node
                assertEquals("found", parser.currentName());
                parser.nextToken();
                assertFalse(parser.booleanValue());
            }
            assertEquals(Token.END_OBJECT, parser.nextToken());
            parser.nextToken();
            assertEquals(Token.END_OBJECT, parser.currentToken());
        }
        assertEquals(Token.END_ARRAY, parser.nextToken());
    }

    private void verifyNodeDecisions(XContentParser parser, Map<String, AllocationDecision> expectedNodeDecisions,
                                     boolean includeYesDecisions, boolean reuseStore) throws IOException {
        parser.nextToken();
        assertEquals("node_allocation_decisions", parser.currentName());
        assertEquals(Token.START_ARRAY, parser.nextToken());
        boolean encounteredNo = false;
        final int numNodes = expectedNodeDecisions.size();
        for (int i = 0; i < numNodes; i++) {
            String nodeName = verifyNodeDecisionPrologue(parser);
            AllocationDecision allocationDecision = expectedNodeDecisions.get(nodeName);
            assertEquals(allocationDecision.toString(), parser.text());
            if (allocationDecision != AllocationDecision.YES) {
                encounteredNo = true;
            } else {
                assertFalse("encountered a YES node decision after a NO node decision - sort order is wrong", encounteredNo);
            }
            parser.nextToken();
            if ("store".equals(parser.currentName())) {
                assertTrue("store info should not be present", reuseStore);
                assertEquals(Token.START_OBJECT, parser.nextToken());
                parser.nextToken();
                assertEquals("matching_size_in_bytes", parser.currentName());
                parser.nextToken();
                assertThat(parser.longValue(), greaterThan(0L));
                assertEquals(Token.END_OBJECT, parser.nextToken());
                parser.nextToken();
            }
            if (reuseStore == false) {
                assertEquals("weight_ranking", parser.currentName());
                parser.nextToken();
                assertThat(parser.intValue(), greaterThan(0));
                parser.nextToken();
            }
            if (allocationDecision == AllocationDecision.NO || allocationDecision == AllocationDecision.THROTTLED || includeYesDecisions) {
                assertEquals("deciders", parser.currentName());
                boolean atLeastOneMatchingDecisionFound = verifyDeciders(parser, allocationDecision);
                parser.nextToken();
                if (allocationDecision == AllocationDecision.NO || allocationDecision == AllocationDecision.THROTTLED) {
                    assertTrue("decision was " + allocationDecision + " but found no node's with that decision",
                        atLeastOneMatchingDecisionFound);
                }
            }
            assertEquals(Token.END_OBJECT, parser.currentToken());
        }
        assertEquals(Token.END_ARRAY, parser.nextToken());
    }

    private String verifyNodeDecisionPrologue(XContentParser parser) throws IOException {
        assertEquals(Token.START_OBJECT, parser.nextToken());
        parser.nextToken();
        assertEquals("node_id", parser.currentName());
        parser.nextToken();
        assertNotNull(parser.text());
        parser.nextToken();
        assertEquals("node_name", parser.currentName());
        parser.nextToken();
        String nodeName = parser.text();
        assertNotNull(nodeName);
        parser.nextToken();
        assertEquals("transport_address", parser.currentName());
        parser.nextToken();
        assertNotNull(parser.text());
        parser.nextToken();
        assertEquals("node_decision", parser.currentName());
        parser.nextToken();
        return nodeName;
    }

    private boolean verifyDeciders(XContentParser parser, AllocationDecision allocationDecision) throws IOException {
        assertEquals(Token.START_ARRAY, parser.nextToken());
        boolean atLeastOneMatchingDecisionFound = false;
        while (parser.nextToken() != Token.END_ARRAY) {
            assertEquals(Token.START_OBJECT, parser.currentToken());
            parser.nextToken();
            assertEquals("decider", parser.currentName());
            parser.nextToken();
            assertNotNull(parser.text());
            parser.nextToken();
            assertEquals("decision", parser.currentName());
            parser.nextToken();
            String decisionText = parser.text();
            if ((allocationDecision == AllocationDecision.NO && decisionText.equals("NO")
                     || (allocationDecision == AllocationDecision.THROTTLED && decisionText.equals("THROTTLE")))) {
                atLeastOneMatchingDecisionFound = true;
            }
            assertNotNull(decisionText);
            parser.nextToken();
            assertEquals("explanation", parser.currentName());
            parser.nextToken();
            assertNotNull(parser.text());
            assertEquals(Token.END_OBJECT, parser.nextToken());
        }
        return atLeastOneMatchingDecisionFound;
    }

    private Map<String, AllocationDecision> allNodeDecisions(AllocationDecision allocationDecision, boolean removePrimary) {
        Map<String, AllocationDecision> nodeDecisions = new HashMap<>();
        Set<String> allNodes = Sets.newHashSet(internalCluster().getNodeNames());
        allNodes.remove(removePrimary ? primaryNodeName() : replicaNode().getName());
        for (String nodeName : allNodes) {
            nodeDecisions.put(nodeName, allocationDecision);
        }
        return nodeDecisions;
    }

}
