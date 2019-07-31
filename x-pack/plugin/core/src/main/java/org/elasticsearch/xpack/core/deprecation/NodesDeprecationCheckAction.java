/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Runs deprecation checks on each node. Deprecation checks are performed locally so that filtered settings
 * can be accessed in the deprecation checks.
 */
public class NodesDeprecationCheckAction extends ActionType<NodesDeprecationCheckResponse> {
    public static final NodesDeprecationCheckAction INSTANCE = new NodesDeprecationCheckAction();
    public static final String NAME = "cluster:admin/xpack/deprecation/nodes/info";

    private NodesDeprecationCheckAction() {
        super(NAME, NodesDeprecationCheckResponse::new);
    }

    public static class NodeRequest extends BaseNodeRequest {

        NodesDeprecationCheckRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new NodesDeprecationCheckRequest(in);
        }
        public NodeRequest(NodesDeprecationCheckRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {
        private List<DeprecationIssue> deprecationIssues;

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            deprecationIssues = in.readList(DeprecationIssue::new);
        }

        public NodeResponse(DiscoveryNode node, List<DeprecationIssue> deprecationIssues) {
            super(node);
            this.deprecationIssues = deprecationIssues;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(this.deprecationIssues);
        }

        public List<DeprecationIssue> getDeprecationIssues() {
            return deprecationIssues;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeResponse that = (NodeResponse) o;
            return Objects.equals(getDeprecationIssues(), that.getDeprecationIssues())
                && Objects.equals(getNode(), that.getNode());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNode(), getDeprecationIssues());
        }
    }

    public static class RequestBuilder extends NodesOperationRequestBuilder<NodesDeprecationCheckRequest,
        NodesDeprecationCheckResponse, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client,
                                 ActionType<NodesDeprecationCheckResponse> action,
                                 NodesDeprecationCheckRequest request) {
            super(client, action, request);
        }
    }
}
