/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestNodesActionTests extends ESTestCase {

    private RestNodesAction action;

    @Before
    public void setUpAction() {
        action = new RestNodesAction();
    }

    public void testBuildTableDoesNotThrowGivenNullNodeInfoAndStats() {
        ClusterName clusterName = new ClusterName("cluster-1");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        builder.add(DiscoveryNodeUtils.builder("node-1").roles(emptySet()).build());
        DiscoveryNodes discoveryNodes = builder.build();
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        ClusterStateResponse clusterStateResponse = new ClusterStateResponse(clusterName, clusterState, false);
        NodesInfoResponse nodesInfoResponse = new NodesInfoResponse(clusterName, Collections.emptyList(), Collections.emptyList());
        NodesStatsResponse nodesStatsResponse = new NodesStatsResponse(clusterName, Collections.emptyList(), Collections.emptyList());

        action.buildTable(false, new FakeRestRequest(), clusterStateResponse, nodesInfoResponse, nodesStatsResponse);
    }

    public void testFormattedNumericSort() {
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("node-1")).add(DiscoveryNodeUtils.create("node-2")))
            .build();

        final var nowMillis = System.currentTimeMillis();
        final var rowOrder = RestTable.getRowOrder(
            action.buildTable(
                false,
                new FakeRestRequest(),
                new ClusterStateResponse(clusterState.getClusterName(), clusterState, false),
                new NodesInfoResponse(clusterState.getClusterName(), List.of(), List.of()),
                new NodesStatsResponse(
                    clusterState.getClusterName(),
                    List.of(
                        // sorting 10 vs 9 in all relevant columns, since these sort incorrectly as strings
                        getTrickySortingNodeStats(nowMillis, clusterState.nodes().get("node-1"), 10),
                        getTrickySortingNodeStats(nowMillis, clusterState.nodes().get("node-2"), 9)
                    ),
                    Collections.emptyList()
                )
            ),
            new FakeRestRequest.Builder(xContentRegistry()).withParams(
                Map.of("s", randomFrom("load_1m", "load_5m", "load_15m", "disk.used_percent"))
            ).build()
        );

        final var nodesList = new ArrayList<DiscoveryNode>();
        for (final var node : clusterState.nodes()) {
            nodesList.add(node);
        }

        assertEquals("node-2", nodesList.get(rowOrder.get(0)).getId());
        assertEquals("node-1", nodesList.get(rowOrder.get(1)).getId());
    }

    private static NodeStats getTrickySortingNodeStats(long nowMillis, DiscoveryNode node, int sortValue) {
        return new NodeStats(
            node,
            nowMillis,
            null,
            new OsStats(
                nowMillis,
                new OsStats.Cpu((short) sortValue, new double[] { sortValue, sortValue, sortValue }),
                new OsStats.Mem(0, 0, 0),
                new OsStats.Swap(0, 0),
                null
            ),
            null,
            null,
            null,
            new FsInfo(nowMillis, null, new FsInfo.Path[] { new FsInfo.Path("/foo", "/foo", 100, 100 - sortValue, 100 - sortValue) }),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }
}
