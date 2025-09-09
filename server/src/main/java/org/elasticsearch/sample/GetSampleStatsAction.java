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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetSampleStatsAction extends ActionType<GetSampleStatsAction.Response> {

    public static final GetSampleStatsAction INSTANCE = new GetSampleStatsAction();
    public static final String NAME = "indices:admin/sample/stats";

    private GetSampleStatsAction() {
        super(NAME);
    }

    public static class Response extends BaseNodesResponse<GetSampleStatsAction.NodeResponse> implements Writeable, ToXContentObject {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(ClusterName clusterName, List<GetSampleStatsAction.NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        public SamplingService.SampleStats getSampleStats() {
            return getNodes().stream()
                .map(n -> n.sampleStats)
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
            GetSampleStatsAction.Response that = (GetSampleStatsAction.Response) o;
            return Objects.equals(getNodes(), that.getNodes()) && Objects.equals(failures(), that.failures());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNodes(), failures());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return getSampleStats().toXContent(builder, params);
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
            GetSampleStatsAction.NodeResponse that = (GetSampleStatsAction.NodeResponse) o;
            return sampleStats.equals(that.sampleStats); // TODO
        }

        @Override
        public int hashCode() {
            return Objects.hash(sampleStats); // TODO
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
