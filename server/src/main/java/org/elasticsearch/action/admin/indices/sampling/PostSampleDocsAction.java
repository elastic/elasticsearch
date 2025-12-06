/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Action for manually adding documents to a sample.
 * <p>
 * This action allows users to POST an array of documents directly to the sample
 * without going through the normal sampling process (no rate or condition checks).
 * </p>
 */
public class PostSampleDocsAction extends ActionType<PostSampleDocsAction.Response> {

    public static final PostSampleDocsAction INSTANCE = new PostSampleDocsAction();
    public static final String NAME = "indices:admin/sample/docs/post";

    private PostSampleDocsAction() {
        super(NAME);
    }

    /**
     * Request to add documents to a sample.
     */
    public static class Request extends BaseNodesRequest implements IndicesRequest.Replaceable {
        private String indexName;
        private final List<Map<String, Object>> documents;

        public Request(String indexName, List<Map<String, Object>> documents) {
            super((String[]) null);
            this.indexName = indexName;
            this.documents = documents;
        }

        public Request(StreamInput in) throws IOException {
            this.indexName = in.readString();
            this.documents = in.readCollectionAsList(StreamInput::readGenericMap);
        }

        public void writeToInternal(StreamOutput out) throws IOException {
            out.writeString(indexName);
            out.writeCollection(documents, StreamOutput::writeGenericMap);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "post sample docs", parentTaskId, headers);
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (this.indexName.contains("*")) {
                return (ActionRequestValidationException) new ActionRequestValidationException().addValidationError(
                    "Wildcards are not supported, but found [" + indexName + "]"
                );
            }
            if (documents == null || documents.isEmpty()) {
                return (ActionRequestValidationException) new ActionRequestValidationException().addValidationError(
                    "Documents array cannot be null or empty"
                );
            }
            return null;
        }

        @Override
        public Request indices(String... indices) {
            assert indices.length == 1 : "PostSampleDocsAction only supports a single index name";
            this.indexName = indices[0];
            return this;
        }

        @Override
        public String[] indices() {
            return new String[] { indexName };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_ALLOW_SELECTORS;
        }

        public String getIndexName() {
            return indexName;
        }

        public List<Map<String, Object>> getDocuments() {
            return documents;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(indexName, request.indexName) && Objects.equals(documents, request.documents);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, documents);
        }
    }

    /**
     * Node-level request for adding documents to a sample.
     */
    public static class NodeRequest extends AbstractTransportRequest implements IndicesRequest {
        private final String indexName;
        private final List<Map<String, Object>> documents;

        public NodeRequest(String indexName, List<Map<String, Object>> documents) {
            this.indexName = indexName;
            this.documents = documents;
        }

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.indexName = in.readString();
            this.documents = in.readCollectionAsList(StreamInput::readGenericMap);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(indexName);
            out.writeCollection(documents, StreamOutput::writeGenericMap);
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public String[] indices() {
            return new String[] { indexName };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_ALLOW_SELECTORS;
        }

        public String getIndexName() {
            return indexName;
        }

        public List<Map<String, Object>> getDocuments() {
            return documents;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeRequest that = (NodeRequest) o;
            return Objects.equals(indexName, that.indexName) && Objects.equals(documents, that.documents);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, documents);
        }
    }

    /**
     * Response from a single node after adding documents.
     */
    public static class NodeResponse extends BaseNodeResponse {
        private final int added;
        private final int rejected;
        private final List<String> failures;

        public NodeResponse(DiscoveryNode node, int added, int rejected, List<String> failures) {
            super(node);
            this.added = added;
            this.rejected = rejected;
            this.failures = failures;
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            this.added = in.readInt();
            this.rejected = in.readInt();
            this.failures = in.readStringCollectionAsList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(added);
            out.writeInt(rejected);
            out.writeStringCollection(failures);
        }

        public int getAdded() {
            return added;
        }

        public int getRejected() {
            return rejected;
        }

        public List<String> getFailures() {
            return failures;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeResponse that = (NodeResponse) o;
            return added == that.added
                && rejected == that.rejected
                && getNode().equals(that.getNode())
                && Objects.equals(failures, that.failures);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNode(), added, rejected, failures);
        }
    }

    /**
     * Aggregated response from all nodes.
     */
    public static class Response extends BaseNodesResponse<NodeResponse> implements ToXContentObject {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }

        public int getTotalAdded() {
            return getNodes().stream().mapToInt(NodeResponse::getAdded).sum();
        }

        public int getTotalRejected() {
            return getNodes().stream().mapToInt(NodeResponse::getRejected).sum();
        }

        public List<String> getAllFailures() {
            List<String> allFailures = new ArrayList<>();
            for (NodeResponse nodeResponse : getNodes()) {
                allFailures.addAll(nodeResponse.getFailures());
            }
            for (FailedNodeException failedNode : failures()) {
                allFailures.add(failedNode.getDetailedMessage());
            }
            return allFailures;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.field("added", getTotalAdded());
            builder.field("rejected", getTotalRejected());
            builder.startArray("failures");
            for (String failure : getAllFailures()) {
                builder.value(failure);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(getNodes(), response.getNodes());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNodes());
        }
    }
}
