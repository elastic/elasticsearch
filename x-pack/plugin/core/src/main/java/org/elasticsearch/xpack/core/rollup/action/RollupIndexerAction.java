/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class RollupIndexerAction extends ActionType<RollupIndexerAction.Response> {

    public static final RollupIndexerAction INSTANCE = new RollupIndexerAction();
    public static final String NAME = "indices:admin/xpack/rollup_indexer";

    private RollupIndexerAction() {
        super(NAME, RollupIndexerAction.Response::new);
    }

    public static class Request extends BroadcastRequest<Request> implements IndicesRequest, ToXContentObject {
        private RollupAction.Request rollupRequest;
        private String[] dimensionFields;
        private String[] metricFields;
        private String[] labelFields;

        public Request(
            RollupAction.Request rollupRequest,
            final String[] dimensionFields,
            final String[] metricFields,
            final String[] labelFields
        ) {
            super(rollupRequest.indices());
            this.rollupRequest = rollupRequest;
            this.dimensionFields = dimensionFields;
            this.metricFields = metricFields;
            this.labelFields = labelFields;
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            this.rollupRequest = new RollupAction.Request(in);
            this.dimensionFields = in.readStringArray();
            this.metricFields = in.readStringArray();
            this.labelFields = in.readStringArray();
        }

        @Override
        public String[] indices() {
            return rollupRequest.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            return rollupRequest.indicesOptions();
        }

        public RollupAction.Request getRollupRequest() {
            return rollupRequest;
        }

        public String[] getDimensionFields() {
            return this.dimensionFields;
        }

        public String[] getMetricFields() {
            return this.metricFields;
        }

        public String[] getLabelFields() {
            return labelFields;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new RollupTask(id, type, action, parentTaskId, rollupRequest.getRollupIndex(), rollupRequest.getRollupConfig(), headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            rollupRequest.writeTo(out);
            out.writeStringArray(dimensionFields);
            out.writeStringArray(metricFields);
            out.writeStringArray(labelFields);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("rollup_request", rollupRequest);
            builder.array("dimension_fields", dimensionFields);
            builder.array("metric_fields", metricFields);
            builder.array("label_fields", labelFields);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            int result = rollupRequest.hashCode();
            result = 31 * result + Arrays.hashCode(dimensionFields);
            result = 31 * result + Arrays.hashCode(metricFields);
            result = 31 * result + Arrays.hashCode(labelFields);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            if (rollupRequest.equals(request.rollupRequest) == false) return false;
            if (Arrays.equals(dimensionFields, request.dimensionFields) == false) return false;
            if (Arrays.equals(labelFields, request.labelFields) == false) return false;
            return Arrays.equals(metricFields, request.metricFields);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        protected RequestBuilder(ElasticsearchClient client, RollupIndexerAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends BroadcastResponse implements Writeable, ToXContentObject {
        private final boolean created;

        private final long numIndexed;

        public Response(boolean created, int totalShards, int successfulShards, int failedShards, long numIndexed) {
            super(totalShards, successfulShards, failedShards, null);
            this.created = created;
            this.numIndexed = numIndexed;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            created = in.readBoolean();
            numIndexed = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(created);
            out.writeLong(numIndexed);
        }

        public boolean isCreated() {
            return created;
        }

        public long getNumIndexed() {
            return numIndexed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("created", created);
            builder.field("indexed", numIndexed);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof Response) == false) return false;

            Response response = (Response) o;

            if (created != response.created) return false;
            return numIndexed == response.numIndexed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(created, numIndexed);
        }
    }

    /**
     * Internal rollup request executed directly against a specific index shard.
     */
    public static class ShardRollupRequest extends BroadcastShardRequest {
        private final Request request;

        public ShardRollupRequest(StreamInput in) throws IOException {
            super(in);
            this.request = new Request(in);
        }

        public ShardRollupRequest(ShardId shardId, Request request) {
            super(shardId, request);
            this.request = request;
        }

        public String getRollupIndex() {
            return request.getRollupRequest().getRollupIndex();
        }

        public RollupActionConfig getRollupConfig() {
            return request.getRollupRequest().getRollupConfig();
        }

        public String[] getDimensionFields() {
            return request.getDimensionFields();
        }

        public String[] getMetricFields() {
            return request.getMetricFields();
        }

        public String[] getLabelFields() {
            return request.getLabelFields();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    public static class ShardRollupResponse extends BroadcastShardResponse {

        private final long numIndexed;

        public ShardRollupResponse(ShardId shardId, long numIndexed) {
            super(shardId);
            this.numIndexed = numIndexed;
        }

        public ShardRollupResponse(StreamInput in) throws IOException {
            super(in);
            numIndexed = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(numIndexed);
        }

        public long getNumIndexed() {
            return numIndexed;
        }
    }
}
