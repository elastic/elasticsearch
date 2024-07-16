/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.InferenceRequestStats;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetInternalInferenceUsageAction extends ActionType<GetInternalInferenceUsageAction.Response> {

    public static final GetInternalInferenceUsageAction INSTANCE = new GetInternalInferenceUsageAction();
    public static final String NAME = "cluster:monitor/xpack/inference/internal_usage/get";

    public GetInternalInferenceUsageAction() {
        super(NAME);
    }

    public static class Request extends BaseNodesRequest<Request> {

        public Request() {
            super((String[]) null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            // The class doesn't have any members at the moment so return the same hash code
            return Objects.hash(NAME);
        }
    }

    public static class NodeRequest extends TransportRequest {
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        public NodeRequest() {}
    }

    public static class Response extends BaseNodesResponse<NodeResponse> implements Writeable, ToXContentObject {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        // TODO do we need this?
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            for (var entry : getNodesMap().entrySet()) {
                NodeResponse response = entry.getValue();

                builder.startObject(entry.getKey());
                response.toXContent(builder, params);
                builder.endObject();
            }

            builder.endObject();
            return builder;
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return Objects.equals(getNodes(), that.getNodes()) && Objects.equals(failures(), that.failures());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNodes(), failures());
        }
    }

    public static class NodeResponse extends BaseNodeResponse implements ToXContentFragment {
        static final String REQUEST_STATS_FIELD_NAME = "requests";

        private final Map<String, InferenceRequestStats> inferenceRequestStats;

        public NodeResponse(DiscoveryNode node, Map<String, InferenceRequestStats> inferenceRequestStats) {
            super(node);
            this.inferenceRequestStats = Objects.requireNonNull(inferenceRequestStats);
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);

            inferenceRequestStats = in.readImmutableMap(StreamInput::readString, InferenceRequestStats::new);
        }

        public Map<String, InferenceRequestStats> getInferenceRequestStats() {
            return Collections.unmodifiableMap(inferenceRequestStats);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(inferenceRequestStats, StreamOutput::writeString, StreamOutput::writeWriteable);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(REQUEST_STATS_FIELD_NAME, inferenceRequestStats);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeResponse response = (NodeResponse) o;
            return Objects.equals(inferenceRequestStats, response.inferenceRequestStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceRequestStats);
        }
    }
}
