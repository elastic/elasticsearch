/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Defines the request/response types for {@link TransportNodeUsageStatsForThreadPoolsAction}.
 */
public class NodeUsageStatsForThreadPoolsAction {
    /**
     * The sender request type that will be resolved to send individual {@link NodeRequest} requests to every node in the cluster.
     */
    public static class Request extends BaseNodesRequest {
        /**
         * @param nodeIds The list of nodes to which to send individual requests and collect responses from. If the list is null, all nodes
         *                in the cluster will be sent a request.
         */
        public Request(String[] nodeIds) {
            super(nodeIds);
        }
    }

    /**
     * Request sent to and received by a cluster node. There are no parameters needed in the node-specific request.
     */
    public static class NodeRequest extends AbstractTransportRequest {
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        public NodeRequest() {}
    }

    /**
     * A collection of {@link NodeUsageStatsForThreadPools} responses from all the cluster nodes.
     */
    public static class Response extends BaseNodesResponse<NodeUsageStatsForThreadPoolsAction.NodeResponse> {

        protected Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(
            ClusterName clusterName,
            List<NodeUsageStatsForThreadPoolsAction.NodeResponse> nodeResponses,
            List<FailedNodeException> nodeFailures
        ) {
            super(clusterName, nodeResponses, nodeFailures);
        }

        /**
         * Combines the responses from each node that was called into a single map (by node ID) for the final {@link Response}.
         */
        public Map<String, NodeUsageStatsForThreadPools> getAllNodeUsageStatsForThreadPools() {
            Map<String, NodeUsageStatsForThreadPools> allNodeUsageStatsForThreadPools = new HashMap<>();
            for (NodeUsageStatsForThreadPoolsAction.NodeResponse nodeResponse : getNodes()) {
                allNodeUsageStatsForThreadPools.put(
                    nodeResponse.getNodeUsageStatsForThreadPools().nodeId(),
                    nodeResponse.getNodeUsageStatsForThreadPools()
                );
            }
            return allNodeUsageStatsForThreadPools;
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodeResponses) throws IOException {
            out.writeCollection(nodeResponses);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeUsageStatsForThreadPoolsAction.NodeResponse::new);
        }

        @Override
        public String toString() {
            return "NodeUsageStatsForThreadPoolsAction.Response{" + getNodes() + "}";
        }
    }

    /**
     * A {@link NodeUsageStatsForThreadPools} response from a single cluster node.
     */
    public static class NodeResponse extends BaseNodeResponse {
        private final NodeUsageStatsForThreadPools nodeUsageStatsForThreadPools;

        protected NodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
            super(in, node);
            this.nodeUsageStatsForThreadPools = new NodeUsageStatsForThreadPools(in);
        }

        public NodeResponse(DiscoveryNode node, NodeUsageStatsForThreadPools nodeUsageStatsForThreadPools) {
            super(node);
            this.nodeUsageStatsForThreadPools = nodeUsageStatsForThreadPools;
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            this.nodeUsageStatsForThreadPools = new NodeUsageStatsForThreadPools(in);
        }

        public NodeUsageStatsForThreadPools getNodeUsageStatsForThreadPools() {
            return nodeUsageStatsForThreadPools;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            nodeUsageStatsForThreadPools.writeTo(out);
        }

        @Override
        public String toString() {
            return "NodeUsageStatsForThreadPoolsAction.NodeResponse{"
                + "nodeId="
                + getNode().getId()
                + ", nodeUsageStatsForThreadPools="
                + nodeUsageStatsForThreadPools
                + "}";
        }
    }

}
