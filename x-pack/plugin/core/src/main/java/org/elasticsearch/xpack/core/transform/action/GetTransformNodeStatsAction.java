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

import java.io.IOException;
import java.util.List;

public class GetTransformNodeStatsAction extends ActionType<GetTransformNodeStatsAction.NodesStatsResponse> {

    public static final GetTransformNodeStatsAction INSTANCE = new GetTransformNodeStatsAction();
    public static final String NAME = "cluster:admin/transform/node_stats";

    private static final String TOTAL_FIELD_NAME = "total";
    private static final String REGISTERED_TRANSFORM_COUNT_FIELD_NAME = "registered_transform_count";
    private static final String PEEK_TRANSFORM_FIELD_NAME = "peek_transform";

    private GetTransformNodeStatsAction() {
        super(NAME);
    }

    public static class NodesStatsRequest extends BaseNodesRequest<NodesStatsRequest> {

        public NodesStatsRequest() {
            super(Strings.EMPTY_ARRAY);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }

    public static class NodesStatsResponse extends BaseNodesResponse<NodeStatsResponse> implements ToXContentObject {

        public int getTotalRegisteredTransformCount() {
            int totalRegisteredTransformCount = 0;
            for (var nodeResponse : getNodes()) {
                totalRegisteredTransformCount += nodeResponse.getRegisteredTransformCount();
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
            builder.field(REGISTERED_TRANSFORM_COUNT_FIELD_NAME, getTotalRegisteredTransformCount());
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

        private final int registeredTransformCount;
        private final String peekTransformName;

        public int getRegisteredTransformCount() {
            return this.registeredTransformCount;
        }

        public NodeStatsResponse(DiscoveryNode node, int registeredTransformCount) {
            this(node, registeredTransformCount, null);
        }

        public NodeStatsResponse(DiscoveryNode node, int registeredTransformCount, String peekTransformName) {
            super(node);
            this.registeredTransformCount = registeredTransformCount;
            this.peekTransformName = peekTransformName;
        }

        public NodeStatsResponse(StreamInput in) throws IOException {
            super(in);
            this.registeredTransformCount = in.readVInt();
            this.peekTransformName = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(this.registeredTransformCount);
            out.writeOptionalString(peekTransformName);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(REGISTERED_TRANSFORM_COUNT_FIELD_NAME, registeredTransformCount);
            builder.field(PEEK_TRANSFORM_FIELD_NAME, peekTransformName);
            return builder.endObject();
        }
    }
}
