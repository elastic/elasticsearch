/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation.stats;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class NodesDataTiersUsageAction extends ActionType<NodesDataTiersUsageAction.NodesResponse> {

    public static final NodesDataTiersUsageAction INSTANCE = new NodesDataTiersUsageAction();
    public static final String NAME = "cluster:monitor/nodes/data_tier_usage";

    private NodesDataTiersUsageAction() {
        super(NAME, NodesResponse::new);
    }

    public static class NodesRequest extends BaseNodesRequest<NodesRequest> {

        public NodesRequest() {
            super((String[]) null);
        }

        public NodesRequest(StreamInput in) throws IOException {
            super(in);
        }

        /**
         * Get stats from nodes based on the nodes ids specified. If none are passed, stats
         * for all nodes will be returned.
         */
        public NodesRequest(String... nodesIds) {
            super(nodesIds);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class NodeRequest extends TransportRequest {

        static final NodeRequest INSTANCE = new NodeRequest();

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        public NodeRequest() {

        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class NodesResponse extends BaseNodesResponse<NodeDataTiersUsage> {

        public NodesResponse(StreamInput in) throws IOException {
            super(in);
        }

        public NodesResponse(ClusterName clusterName, List<NodeDataTiersUsage> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeDataTiersUsage> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeDataTiersUsage::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeDataTiersUsage> nodes) throws IOException {
            out.writeCollection(nodes);
        }
    }
}
