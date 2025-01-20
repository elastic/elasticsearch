/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.action.GetTransformNodeStatsAction.NodeStatsResponse;
import org.elasticsearch.xpack.core.transform.action.GetTransformNodeStatsAction.NodesStatsResponse;
import org.elasticsearch.xpack.core.transform.transforms.TransformSchedulerStats;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GetTransformNodeStatsActionNodesStatsResponseTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("my-cluster");

    public void testEmptyResponse() {
        var nodesStatsResponse = new NodesStatsResponse(CLUSTER_NAME, List.of(), List.of());
        assertThat(nodesStatsResponse.getNodes(), is(empty()));
        assertThat(nodesStatsResponse.failures(), is(empty()));
        assertThat(nodesStatsResponse.getTotalRegisteredTransformCount(), is(equalTo(0)));
    }

    public void testResponse() {
        var nodeA = new NodeStatsResponse(createNode("node-A"), new TransformSchedulerStats(7, null));
        var nodeB = new NodeStatsResponse(createNode("node-B"), new TransformSchedulerStats(0, null));
        var nodeC = new NodeStatsResponse(createNode("node-C"), new TransformSchedulerStats(4, null));

        var nodesStatsResponse = new NodesStatsResponse(CLUSTER_NAME, List.of(nodeA, nodeB, nodeC), List.of());
        assertThat(nodesStatsResponse.getNodes(), containsInAnyOrder(nodeA, nodeB, nodeC));
        assertThat(nodesStatsResponse.failures(), is(empty()));
        assertThat(nodesStatsResponse.getTotalRegisteredTransformCount(), is(equalTo(11)));
    }

    public void testResponseWithFailure() {
        var nodeA = new NodeStatsResponse(createNode("node-A"), new TransformSchedulerStats(7, null));
        var nodeB = new NodeStatsResponse(createNode("node-B"), new TransformSchedulerStats(0, null));
        var nodeC = new FailedNodeException("node-C", "node C failed", null);

        var nodesStatsResponse = new NodesStatsResponse(CLUSTER_NAME, List.of(nodeA, nodeB), List.of(nodeC));
        assertThat(nodesStatsResponse.getNodes(), containsInAnyOrder(nodeA, nodeB));
        assertThat(nodesStatsResponse.failures(), contains(nodeC));
        assertThat(nodesStatsResponse.getTotalRegisteredTransformCount(), is(equalTo(7)));
    }

    private static DiscoveryNode createNode(String name) {
        return DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID(random())).name(name).build();
    }
}
