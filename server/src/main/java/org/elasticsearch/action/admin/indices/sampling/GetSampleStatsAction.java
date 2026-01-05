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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;

public class GetSampleStatsAction extends ActionType<GetSampleStatsAction.Response> {

    public static final GetSampleStatsAction INSTANCE = new GetSampleStatsAction();
    public static final String NAME = "indices:admin/sample/stats";

    private GetSampleStatsAction() {
        super(NAME);
    }

    public static class Request extends BaseNodesRequest implements IndicesRequest.Replaceable {
        private String indexName;

        public Request(String indexName) {
            super((String[]) null);
            this.indexName = indexName;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "get sample stats", parentTaskId, headers);
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
            return null;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            assert indices.length == 1 : "GetSampleStatsAction only supports a single index name";
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
    }

    public static class NodeRequest extends AbstractTransportRequest implements IndicesRequest {
        private final String indexName;

        public NodeRequest(String indexName) {
            this.indexName = indexName;
        }

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.indexName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(indexName);
        }

        public String getIndexName() {
            return indexName;
        }

        @Override
        public String[] indices() {
            return new String[] { indexName };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_ALLOW_SELECTORS;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeRequest other = (NodeRequest) o;
            return Objects.equals(indexName, other.indexName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName);
        }
    }

    public static class Response extends BaseNodesResponse<GetSampleStatsAction.NodeResponse> implements Writeable, ChunkedToXContent {
        final int maxSize;

        public Response(StreamInput in) throws IOException {
            super(in);
            maxSize = in.readInt();
        }

        public Response(
            ClusterName clusterName,
            List<GetSampleStatsAction.NodeResponse> nodes,
            List<FailedNodeException> failures,
            int maxSize
        ) {
            super(clusterName, nodes, failures);
            this.maxSize = maxSize;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(maxSize);
        }

        public SamplingService.SampleStats getSampleStats() {
            SamplingService.SampleStats rawStats = getRawSampleStats();
            return rawStats.adjustForMaxSize(maxSize);
        }

        private SamplingService.SampleStats getRawSampleStats() {
            return getNodes().stream()
                .map(NodeResponse::getSampleStats)
                .filter(Objects::nonNull)
                .reduce(SamplingService.SampleStats::combine)
                .orElse(new SamplingService.SampleStats());
        }

        @Override
        protected List<GetSampleStatsAction.NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(GetSampleStatsAction.NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<GetSampleStatsAction.NodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
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

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return chunk(getSampleStats());
        }
    }

    public static class NodeResponse extends BaseNodeResponse {
        private final SamplingService.SampleStats sampleStats;

        protected NodeResponse(StreamInput in) throws IOException {
            super(in);
            sampleStats = new SamplingService.SampleStats(in);
        }

        protected NodeResponse(DiscoveryNode node, SamplingService.SampleStats sampleStats) {
            super(node);
            this.sampleStats = sampleStats;
        }

        public SamplingService.SampleStats getSampleStats() {
            return sampleStats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            sampleStats.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetSampleStatsAction.NodeResponse other = (GetSampleStatsAction.NodeResponse) o;
            return getNode().equals(other.getNode()) && sampleStats.equals(other.sampleStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNode(), sampleStats);
        }
    }
}
