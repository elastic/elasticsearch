/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sample;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.collect.Iterators.single;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;

public class GetSampleAction extends ActionType<GetSampleAction.Response> {

    public static final GetSampleAction INSTANCE = new GetSampleAction();
    public static final String NAME = "indices:admin/sample";

    private GetSampleAction() {
        super(NAME);
    }

    public static class Response extends BaseNodesResponse<NodeResponse> implements Writeable, ChunkedToXContent {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        public List<IndexRequest> getSamples() {
            return getNodes().stream().map(n -> n.samples).filter(Objects::nonNull).flatMap(Collection::stream).toList();
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
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                chunk((builder, p) -> builder.startObject().startArray("samples")),
                Iterators.flatMap(getSamples().iterator(), sample -> single((builder, params1) -> {
                    Map<String, Object> source = sample.sourceAsMap();
                    builder.value(source);
                    return builder;
                })),
                chunk((builder, p) -> builder.endArray().endObject())
            );
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

    public static class NodeResponse extends BaseNodeResponse {
        private final List<IndexRequest> samples;

        protected NodeResponse(StreamInput in) throws IOException {
            super(in);
            samples = in.readCollectionAsList(IndexRequest::new);
        }

        protected NodeResponse(DiscoveryNode node, List<IndexRequest> samples) {
            super(node);
            this.samples = samples;
        }

        public List<IndexRequest> getSamples() {
            return samples;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(samples);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeResponse that = (NodeResponse) o;
            return samples.equals(that.samples);
        }

        @Override
        public int hashCode() {
            return Objects.hash(samples);
        }
    }

    public static class Request extends BaseNodesRequest implements IndicesRequest.Replaceable {
        private String[] names;

        public Request(String[] names) {
            super((String[]) null);
            this.names = names;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public ActionRequestValidationException validate() {
            if (this.indices().length != 1) {
                return new ActionRequestValidationException();
            }
            return null;
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
            return IndicesOptions.DEFAULT;
        }
    }

    public static class NodeRequest extends AbstractTransportRequest {
        private final String index;

        public NodeRequest(String index) {
            this.index = index;
        }

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.index = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
        }

        public String getIndex() {
            return index;
        }
    }
}
