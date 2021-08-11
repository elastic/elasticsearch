/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingStateAndReason;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;

public class GetDeploymentStatsActionResponseTests extends AbstractWireSerializingTestCase<GetDeploymentStatsAction.Response> {
    @Override
    protected Writeable.Reader<GetDeploymentStatsAction.Response> instanceReader() {
        return GetDeploymentStatsAction.Response::new;
    }

    @Override
    protected GetDeploymentStatsAction.Response createTestInstance() {
        int numStats = randomIntBetween(0, 2);
        var stats = new ArrayList<GetDeploymentStatsAction.Response.AllocationStats>(numStats);
        for (var i=0; i<numStats; i++) {
            stats.add(randomDeploymentStats());
        }
        stats.sort(Comparator.comparing(s -> s.getModelId()));
        return new GetDeploymentStatsAction.Response(Collections.emptyList(), Collections.emptyList(), stats, stats.size());
    }

    public void testAddFailedRoutes_GivenNoFailures() throws UnknownHostException {
        var response = createTestInstance();
        var modifed = GetDeploymentStatsAction.Response.addFailedRoutes(response,
            Collections.emptyMap(),
            buildNodes("node_foo"));
        assertEquals(response, modifed);
    }

    public void testAddFailedRoutes_GivenNoTaskResponses() throws UnknownHostException {
        var emptyResponse = new GetDeploymentStatsAction.Response(Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList(), 0);

        Map<String, Map<String, RoutingStateAndReason>> badRoutes = new HashMap<>();
        for (var modelId : new String[]{"model1", "model2"}) {
            Map<String, RoutingStateAndReason> nodeRoutes = new HashMap<>();
            for (var nodeId : new String[]{"nodeA", "nodeB"}) {
                nodeRoutes.put(nodeId, new RoutingStateAndReason(RoutingState.FAILED, "failure reason"));
            }
            badRoutes.put(modelId, nodeRoutes);
        }

        DiscoveryNodes nodes = buildNodes("nodeA", "nodeB");
        var modified = GetDeploymentStatsAction.Response.addFailedRoutes(emptyResponse, badRoutes, nodes);
        List<GetDeploymentStatsAction.Response.AllocationStats> results = modified.getStats().results();
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

        List<GetDeploymentStatsAction.Response.AllocationStats.NodeStats> nodeStatsList = new ArrayList<>();
        nodeStatsList.add(GetDeploymentStatsAction.Response.AllocationStats.NodeStats.forStartedState(
                    nodes.get("node1"),
                    randomNonNegativeLong(),
                    randomDoubleBetween(0.0, 100.0, true),
                    Instant.now()
                ));
        nodeStatsList.add(GetDeploymentStatsAction.Response.AllocationStats.NodeStats.forStartedState(
            nodes.get("node2"),
            randomNonNegativeLong(),
            randomDoubleBetween(0.0, 100.0, true),
            Instant.now()
        ));

        var model1 = new GetDeploymentStatsAction.Response.AllocationStats(
            "model1",
            ByteSizeValue.ofBytes(randomNonNegativeLong()),
            nodeStatsList);

        Map<String, Map<String, RoutingStateAndReason>> badRoutes = new HashMap<>();
        Map<String, RoutingStateAndReason> nodeRoutes = new HashMap<>();
        nodeRoutes.put("node3", new RoutingStateAndReason(RoutingState.FAILED, "failed on node3"));
        badRoutes.put("model1", nodeRoutes);

        var response = new GetDeploymentStatsAction.Response(Collections.emptyList(), Collections.emptyList(),
            List.of(model1), 1);

        var modified = GetDeploymentStatsAction.Response.addFailedRoutes(response, badRoutes, nodes);
        List<GetDeploymentStatsAction.Response.AllocationStats> results = modified.getStats().results();
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

        List<GetDeploymentStatsAction.Response.AllocationStats.NodeStats> nodeStatsList = new ArrayList<>();
        nodeStatsList.add(GetDeploymentStatsAction.Response.AllocationStats.NodeStats.forStartedState(
            nodes.get("node1"),
            randomNonNegativeLong(),
            randomDoubleBetween(0.0, 100.0, true),
            Instant.now()
        ));
        nodeStatsList.add(GetDeploymentStatsAction.Response.AllocationStats.NodeStats.forStartedState(
            nodes.get("node2"),
            randomNonNegativeLong(),
            randomDoubleBetween(0.0, 100.0, true),
            Instant.now()
        ));

        var model1 = new GetDeploymentStatsAction.Response.AllocationStats(
            "model1",
            ByteSizeValue.ofBytes(randomNonNegativeLong()),
            nodeStatsList);
        var response = new GetDeploymentStatsAction.Response(Collections.emptyList(), Collections.emptyList(),
            List.of(model1), 1);

        // failed state for node 2 conflicts with the task response
        Map<String, Map<String, RoutingStateAndReason>> badRoutes = new HashMap<>();
        Map<String, RoutingStateAndReason> nodeRoutes = new HashMap<>();
        nodeRoutes.put("node2", new RoutingStateAndReason(RoutingState.FAILED, "failed on node3"));
        badRoutes.put("model1", nodeRoutes);

        var modified = GetDeploymentStatsAction.Response.addFailedRoutes(response, badRoutes, nodes);
        List<GetDeploymentStatsAction.Response.AllocationStats> results = modified.getStats().results();
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getNodeStats(), hasSize(2));
        assertEquals("node1", results.get(0).getNodeStats().get(0).getNode().getId());
        assertEquals(RoutingState.STARTED, results.get(0).getNodeStats().get(0).getRoutingState().getState());
        assertEquals("node2", results.get(0).getNodeStats().get(1).getNode().getId());
        // routing state from the bad routes map is chosen to resolve teh conflict
        assertEquals(RoutingState.FAILED, results.get(0).getNodeStats().get(1).getRoutingState().getState());
    }

    private DiscoveryNodes buildNodes(String ... nodeIds) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByAddress(new byte[]{(byte) 192, (byte) 168, (byte) 0, (byte) 1});
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        int port = 9200;
        for (String nodeId : nodeIds) {
            builder.add(new DiscoveryNode(nodeId, new TransportAddress(inetAddress, port++), Version.CURRENT));
        }
        return builder.build();
    }

    private GetDeploymentStatsAction.Response.AllocationStats randomDeploymentStats() {
        List<GetDeploymentStatsAction.Response.AllocationStats.NodeStats> nodeStatsList = new ArrayList<>();
        int numNodes = randomIntBetween(1, 4);
        for (int i = 0; i < numNodes; i++) {
            var node = new DiscoveryNode("node_" + i, new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT);
            if (randomBoolean()) {
                nodeStatsList.add(GetDeploymentStatsAction.Response.AllocationStats.NodeStats.forStartedState(
                    node,
                    randomNonNegativeLong(),
                    randomDoubleBetween(0.0, 100.0, true),
                    Instant.now()
                ));
            } else {
                nodeStatsList.add(GetDeploymentStatsAction.Response.AllocationStats.NodeStats.forNotStartedState(
                    node, randomFrom(RoutingState.values()), randomBoolean() ? null : "a good reason"
                ));
            }
        }

        nodeStatsList.sort(Comparator.comparing(n -> n.getNode().getId()));

        return new GetDeploymentStatsAction.Response.AllocationStats(
            randomAlphaOfLength(5),
            ByteSizeValue.ofBytes(randomNonNegativeLong()),
            nodeStatsList);
    }
}
