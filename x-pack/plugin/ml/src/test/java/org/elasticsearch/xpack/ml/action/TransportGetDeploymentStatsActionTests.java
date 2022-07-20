/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsActionResponseTests;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStatsTests;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;

public class TransportGetDeploymentStatsActionTests extends ESTestCase {

    public void testAddFailedRoutes_GivenNoFailures() throws UnknownHostException {
        var response = GetDeploymentStatsActionResponseTests.createRandom();
        var modified = TransportGetDeploymentStatsAction.addFailedRoutes(response, Collections.emptyMap(), buildNodes("node_foo"));
        assertEquals(response, modified);
    }

    public void testAddFailedRoutes_GivenNoTaskResponses() throws UnknownHostException {
        var emptyResponse = new GetDeploymentStatsAction.Response(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            0
        );

        Map<TrainedModelAssignment, Map<String, RoutingInfo>> badRoutes = new HashMap<>();
        for (var modelId : new String[] { "model1", "model2" }) {
            TrainedModelAssignment assignment = createAssignment(modelId);
            Map<String, RoutingInfo> nodeRoutes = new HashMap<>();
            for (var nodeId : new String[] { "nodeA", "nodeB" }) {
                nodeRoutes.put(nodeId, new RoutingInfo(1, 1, RoutingState.FAILED, "failure reason"));
            }
            badRoutes.put(assignment, nodeRoutes);
        }

        DiscoveryNodes nodes = buildNodes("nodeA", "nodeB");
        var modified = TransportGetDeploymentStatsAction.addFailedRoutes(emptyResponse, badRoutes, nodes);
        List<AssignmentStats> results = modified.getStats().results();
        assertThat(results, hasSize(2));
        assertEquals("model1", results.get(0).getModelId());
        assertThat(results.get(0).getNodeStats(), hasSize(2));
        assertEquals("nodeA", results.get(0).getNodeStats().get(0).getNode().getId());
        assertEquals("nodeB", results.get(0).getNodeStats().get(1).getNode().getId());
        assertEquals("nodeA", results.get(1).getNodeStats().get(0).getNode().getId());
        assertEquals("nodeB", results.get(1).getNodeStats().get(1).getNode().getId());
    }

    public void testAddFailedRoutes_GivenMixedResponses() throws UnknownHostException {
        DiscoveryNodes nodes = buildNodes("node1", "node2", "node3");

        List<AssignmentStats.NodeStats> nodeStatsList = new ArrayList<>();
        nodeStatsList.add(AssignmentStatsTests.randomNodeStats(nodes.get("node1")));
        nodeStatsList.add(AssignmentStatsTests.randomNodeStats(nodes.get("node2")));

        var model1 = new AssignmentStats(
            "model1",
            randomBoolean() ? null : randomIntBetween(1, 8),
            randomBoolean() ? null : randomIntBetween(1, 8),
            randomBoolean() ? null : randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomLongBetween(1, 1000000)),
            Instant.now(),
            nodeStatsList
        );

        Map<TrainedModelAssignment, Map<String, RoutingInfo>> badRoutes = new HashMap<>();
        Map<String, RoutingInfo> nodeRoutes = new HashMap<>();
        nodeRoutes.put("node3", new RoutingInfo(1, 1, RoutingState.FAILED, "failed on node3"));
        badRoutes.put(createAssignment("model1"), nodeRoutes);

        var response = new GetDeploymentStatsAction.Response(Collections.emptyList(), Collections.emptyList(), List.of(model1), 1);

        var modified = TransportGetDeploymentStatsAction.addFailedRoutes(response, badRoutes, nodes);
        List<AssignmentStats> results = modified.getStats().results();
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getNodeStats(), hasSize(3));
        assertEquals("node1", results.get(0).getNodeStats().get(0).getNode().getId());
        assertEquals(RoutingState.STARTED, results.get(0).getNodeStats().get(0).getRoutingState().getState());
        assertEquals("node2", results.get(0).getNodeStats().get(1).getNode().getId());
        assertEquals(RoutingState.STARTED, results.get(0).getNodeStats().get(1).getRoutingState().getState());
        assertEquals("node3", results.get(0).getNodeStats().get(2).getNode().getId());
        assertEquals(RoutingState.FAILED, results.get(0).getNodeStats().get(2).getRoutingState().getState());
    }

    public void testAddFailedRoutes_TaskResultIsOverwritten() throws UnknownHostException {
        DiscoveryNodes nodes = buildNodes("node1", "node2");

        List<AssignmentStats.NodeStats> nodeStatsList = new ArrayList<>();
        nodeStatsList.add(AssignmentStatsTests.randomNodeStats(nodes.get("node1")));
        nodeStatsList.add(AssignmentStatsTests.randomNodeStats(nodes.get("node2")));

        var model1 = new AssignmentStats(
            "model1",
            randomBoolean() ? null : randomIntBetween(1, 8),
            randomBoolean() ? null : randomIntBetween(1, 8),
            randomBoolean() ? null : randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomLongBetween(1, 1000000)),
            Instant.now(),
            nodeStatsList
        );
        var response = new GetDeploymentStatsAction.Response(Collections.emptyList(), Collections.emptyList(), List.of(model1), 1);

        // failed state for node 2 conflicts with the task response
        Map<TrainedModelAssignment, Map<String, RoutingInfo>> badRoutes = new HashMap<>();
        Map<String, RoutingInfo> nodeRoutes = new HashMap<>();
        nodeRoutes.put("node2", new RoutingInfo(1, 1, RoutingState.FAILED, "failed on node3"));
        badRoutes.put(createAssignment("model1"), nodeRoutes);

        var modified = TransportGetDeploymentStatsAction.addFailedRoutes(response, badRoutes, nodes);
        List<AssignmentStats> results = modified.getStats().results();
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getNodeStats(), hasSize(2));
        assertEquals("node1", results.get(0).getNodeStats().get(0).getNode().getId());
        assertEquals(RoutingState.STARTED, results.get(0).getNodeStats().get(0).getRoutingState().getState());
        assertEquals("node2", results.get(0).getNodeStats().get(1).getNode().getId());
        // routing state from the bad routes map is chosen to resolve teh conflict
        assertEquals(RoutingState.FAILED, results.get(0).getNodeStats().get(1).getRoutingState().getState());
    }

    private DiscoveryNodes buildNodes(String... nodeIds) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByAddress(new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        int port = 9200;
        for (String nodeId : nodeIds) {
            builder.add(new DiscoveryNode(nodeId, new TransportAddress(inetAddress, port++), Version.CURRENT));
        }
        return builder.build();
    }

    private static TrainedModelAssignment createAssignment(String modelId) {
        return TrainedModelAssignment.Builder.empty(
            new StartTrainedModelDeploymentAction.TaskParams(modelId, 1024, 1, 1, 1, ByteSizeValue.ofBytes(1024))
        ).build();
    }
}
