/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.transform.transforms.TransformSchedulerStats;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.core.transform.transforms.TransformSchedulerStats.REGISTERED_TRANSFORM_COUNT_FIELD_NAME;

public class GetTransformNodeStatsAction extends ActionType<GetTransformNodeStatsAction.NodesStatsResponse> {

    public static final GetTransformNodeStatsAction INSTANCE = new GetTransformNodeStatsAction();
    public static final String NAME = "cluster:admin/transform/node_stats";

    private static final String SCHEDULER_STATS_FIELD_NAME = "scheduler";

    private GetTransformNodeStatsAction() {
        super(NAME);
    }

    public static class NodesStatsRequest extends BaseNodesRequest {
        public NodesStatsRequest() {
            super(Strings.EMPTY_ARRAY);
        }
    }

    public static class NodesStatsResponse extends BaseNodesResponse<NodeStatsResponse> implements ToXContentObject {

        private static final String TOTAL_FIELD_NAME = "total";

        public int getTotalRegisteredTransformCount() {
            int totalRegisteredTransformCount = 0;
            for (var nodeResponse : getNodes()) {
                totalRegisteredTransformCount += nodeResponse.schedulerStats().registeredTransformCount();
            }
            return totalRegisteredTransformCount;
        }

        public NodesStatsResponse(ClusterName clusterName, List<NodeStatsResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        public RestStatus status() {
            return this.hasFailures() ? RestStatus.INTERNAL_SERVER_ERROR : RestStatus.OK;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (var nodeEntry : getNodesMap().entrySet()) {
                String nodeName = nodeEntry.getKey();
                NodeStatsResponse nodeResponse = nodeEntry.getValue();
                builder.field(nodeName);
                nodeResponse.toXContent(builder, params);
            }
            builder.startObject(TOTAL_FIELD_NAME);
            builder.startObject(SCHEDULER_STATS_FIELD_NAME);
            builder.field(REGISTERED_TRANSFORM_COUNT_FIELD_NAME, getTotalRegisteredTransformCount());
            builder.endObject();
            builder.endObject();
            return builder.endObject();
        }

        @Override
        protected List<NodeStatsResponse> readNodesFrom(StreamInput in) throws IOException {
            return TransportAction.localOnly();
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeStatsResponse> nodes) throws IOException {
            TransportAction.localOnly();
        }
    }

    public static class NodeStatsRequest extends TransportRequest {

        public NodeStatsRequest() {}

        public NodeStatsRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class NodeStatsResponse extends BaseNodeResponse implements ToXContentObject {

        private final TransformSchedulerStats schedulerStats;

        public NodeStatsResponse(DiscoveryNode node, TransformSchedulerStats schedulerStats) {
            super(node);
            this.schedulerStats = schedulerStats;
        }

        public NodeStatsResponse(StreamInput in) throws IOException {
            super(in);
            this.schedulerStats = in.readOptionalWriteable(TransformSchedulerStats::new);
        }

        TransformSchedulerStats schedulerStats() {
            return schedulerStats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalWriteable(schedulerStats);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(SCHEDULER_STATS_FIELD_NAME, schedulerStats);
            return builder.endObject();
        }
    }
}
