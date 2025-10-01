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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.collect.Iterators.single;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;

public class GetSampleAction extends ActionType<GetSampleAction.Response> {

    public static final GetSampleAction INSTANCE = new GetSampleAction();
    public static final String NAME = "indices:admin/sample";

    private GetSampleAction() {
        super(NAME);
    }

    public static class Response extends BaseNodesResponse<NodeResponse> implements Writeable, ChunkedToXContent {
        final int maxSize;

        public Response(StreamInput in) throws IOException {
            super(in);
            maxSize = in.readInt();
        }

        public Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures, int maxSize) {
            super(clusterName, nodes, failures);
            this.maxSize = maxSize;
        }

        public List<SamplingService.RawDocument> getSample() {
            return getNodes().stream().map(n -> n.sample).filter(Objects::nonNull).flatMap(Collection::stream).limit(maxSize).toList();
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeResponse::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(maxSize);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                chunk((builder, p) -> builder.startObject().startArray("sample")),
                Iterators.flatMap(getSample().iterator(), rawDocument -> single((builder, params1) -> {
                    builder.startObject();
                    builder.field("index", rawDocument.indexName());
                    Map<String, Object> sourceAsMap = XContentHelper.convertToMap(
                        rawDocument.contentType().xContent(),
                        rawDocument.source(),
                        0,
                        rawDocument.source().length,
                        false
                    );
                    builder.field("source", sourceAsMap);
                    builder.endObject();
                    return builder;
                })),
                chunk((builder, p) -> builder.endArray().endObject())
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response other = (Response) o;
            return Objects.equals(getNodes(), other.getNodes()) && maxSize == other.maxSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNodes(), maxSize);
        }

    }

    public static class NodeResponse extends BaseNodeResponse {
        private final List<SamplingService.RawDocument> sample;

        protected NodeResponse(StreamInput in) throws IOException {
            super(in);
            sample = in.readCollectionAsList(SamplingService.RawDocument::new);
        }

        protected NodeResponse(DiscoveryNode node, List<SamplingService.RawDocument> sample) {
            super(node);
            this.sample = sample;
        }

        public List<SamplingService.RawDocument> getSample() {
            return sample;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(sample);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeResponse other = (NodeResponse) o;
            return getNode().equals(other.getNode()) && sample.equals(other.sample);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNode(), sample);
        }
    }

    public static class Request extends BaseNodesRequest implements IndicesRequest.Replaceable {
        private final ProjectId projectId;
        private String[] names;

        public Request(ProjectId projectId, String[] names) {
            super((String[]) null);
            this.projectId = projectId;
            this.names = names;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "get samples", parentTaskId, headers);
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (this.indices().length != 1) {
                return (ActionRequestValidationException) new ActionRequestValidationException().addValidationError(
                    "Can only get samples for a single index at a time, but found "
                        + Arrays.stream(this.indices()).collect(Collectors.joining(", ", "[", "]"))
                );
            }
            return null;
        }

        public ProjectId getProjectId() {
            return projectId;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.names = indices;
            return this;
        }

        @Override
        public String[] indices() {
            return names;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_ALLOW_SELECTORS;
        }
    }

    public static class NodeRequest extends AbstractTransportRequest implements IndicesRequest {
        private final ProjectId projectId;
        private final String index;

        public NodeRequest(ProjectId projectId, String index) {
            this.projectId = projectId;
            this.index = index;
        }

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.projectId = ProjectId.readFrom(in);
            this.index = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            projectId.writeTo(out);
            out.writeString(index);
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        public ProjectId getProjectId() {
            return projectId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeRequest other = (NodeRequest) o;
            return Objects.equals(projectId, other.projectId) && Objects.equals(index, other.index);
        }

        @Override
        public int hashCode() {
            return Objects.hash(projectId, index);
        }

        @Override
        public String[] indices() {
            return new String[] { index };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_ALLOW_SELECTORS;
        }
    }
}
