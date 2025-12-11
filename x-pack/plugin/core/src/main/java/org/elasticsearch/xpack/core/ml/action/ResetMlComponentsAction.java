/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ResetMlComponentsAction extends ActionType<ResetMlComponentsAction.Response> {

    public static final ResetMlComponentsAction INSTANCE = new ResetMlComponentsAction();
    public static final String NAME = "cluster:internal/xpack/ml/auditor/reset";

    private ResetMlComponentsAction() {
        super(NAME);
    }

    public static class Request extends BaseNodesRequest {

        public static Request RESET_AUDITOR_REQUEST = new Request();

        private Request() {
            super(new String[] { "ml:true" }); // Only ml nodes. See DiscoveryNodes::resolveNodes
        }
    }

    public static class NodeRequest extends AbstractTransportRequest {

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        public NodeRequest() {}

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash();
        }
    }

    public static class Response extends BaseNodesResponse<Response.ResetResponse> {

        public Response(ClusterName clusterName, List<ResetResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        protected Response(StreamInput in) throws IOException {
            super(in);
        }

        public static class ResetResponse extends BaseNodeResponse {
            private final boolean acknowledged;

            public ResetResponse(DiscoveryNode node, boolean acknowledged) {
                super(node);
                this.acknowledged = acknowledged;
            }

            public ResetResponse(StreamInput in) throws IOException {
                super(in, null);
                acknowledged = in.readBoolean();
            }

            public ResetResponse(StreamInput in, DiscoveryNode node) throws IOException {
                super(in, node);
                acknowledged = in.readBoolean();
            }

            public boolean isAcknowledged() {
                return acknowledged;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeBoolean(acknowledged);
            }

            @Override
            public boolean equals(Object o) {
                if (o == null || getClass() != o.getClass()) return false;
                ResetResponse that = (ResetResponse) o;
                return acknowledged == that.acknowledged;
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(acknowledged);
            }
        }

        @Override
        protected List<Response.ResetResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(ResetResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<Response.ResetResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }
    }
}
