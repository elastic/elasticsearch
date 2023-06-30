/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class FollowStatsAction extends ActionType<FollowStatsAction.StatsResponses> {

    public static final String NAME = "cluster:monitor/ccr/follow_stats";

    public static final FollowStatsAction INSTANCE = new FollowStatsAction();

    private FollowStatsAction() {
        super(NAME, FollowStatsAction.StatsResponses::new);
    }

    public static class StatsResponses extends BaseTasksResponse implements ChunkedToXContentObject {

        private final List<StatsResponse> statsResponse;

        public List<StatsResponse> getStatsResponses() {
            return statsResponse;
        }

        public StatsResponses(
            final List<TaskOperationFailure> taskFailures,
            final List<? extends FailedNodeException> nodeFailures,
            final List<StatsResponse> statsResponse
        ) {
            super(taskFailures, nodeFailures);
            this.statsResponse = statsResponse;
        }

        public StatsResponses(StreamInput in) throws IOException {
            super(in);
            statsResponse = in.readList(StatsResponse::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(statsResponse);
        }

        @Override
        public Iterator<ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            // sort by index name, then shard ID
            Transports.assertNotTransportThread("collecting responses into a map may be expensive");
            final Map<String, Map<Integer, StatsResponse>> taskResponsesByIndex = new TreeMap<>();
            for (final StatsResponse response : statsResponse) {
                taskResponsesByIndex.computeIfAbsent(response.status().followerIndex(), k -> new TreeMap<>())
                    .put(response.status().getShardId(), response);
            }
            return innerToXContentChunked(taskResponsesByIndex);
        }

        private static Iterator<ToXContent> innerToXContentChunked(Map<String, Map<Integer, StatsResponse>> taskResponsesByIndex) {
            return Iterators.concat(
                Iterators.single((builder, params) -> builder.startObject().startArray("indices")),
                Iterators.flatMap(
                    taskResponsesByIndex.entrySet().iterator(),
                    indexEntry -> Iterators.concat(
                        Iterators.<ToXContent>single(
                            (builder, params) -> builder
                                .startObject()
                                .field("index", indexEntry.getKey())
                                .field("leader_follower_doc_count_diff", 0)
                                .startArray("shards")
                        ),
                        indexEntry.getValue().values().iterator(),
                        Iterators.single((builder, params) -> builder.endArray().endObject())
                    )
                ),
                Iterators.single((builder, params) -> builder.endArray().endObject())
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StatsResponses that = (StatsResponses) o;
            return Objects.equals(statsResponse, that.statsResponse);
        }

        @Override
        public int hashCode() {
            return Objects.hash(statsResponse);
        }
    }

    public static class StatsRequest extends BaseTasksRequest<StatsRequest> implements IndicesRequest {

        private String[] indices;

        public StatsRequest() {}

        public StatsRequest(StreamInput in) throws IOException {
            super(in);
            indices = in.readOptionalStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(indices);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        public void setIndices(final String[] indices) {
            this.indices = indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictExpand();
        }

        @Override
        public boolean match(final Task task) {
            /*
             * This is a limitation of the current tasks API. When the transport action is executed, the tasks API invokes this match method
             * to find the tasks on which to execute the task-level operation (see TransportTasksAction#nodeOperation and
             * TransportTasksAction#processTasks). If we do the matching here, then we can not match index patterns. Therefore, we override
             * TransportTasksAction#processTasks (see TransportFollowStatsAction#processTasks) and do the matching there. We should never
             * see this method invoked and since we can not support matching a task on the basis of the request here, we throw that this
             * operation is unsupported.
             */
            throw new UnsupportedOperationException();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StatsRequest that = (StatsRequest) o;
            return Arrays.equals(indices, that.indices);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(indices);
        }
    }

    public static class StatsResponse implements Writeable, ToXContentObject {

        private final ShardFollowNodeTaskStatus status;

        public ShardFollowNodeTaskStatus status() {
            return status;
        }

        public StatsResponse(final ShardFollowNodeTaskStatus status) {
            this.status = status;
        }

        public StatsResponse(final StreamInput in) throws IOException {
            this.status = new ShardFollowNodeTaskStatus(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            status.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StatsResponse that = (StatsResponse) o;
            return Objects.equals(status, that.status);
        }

        @Override
        public int hashCode() {
            return Objects.hash(status);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            return status.toXContent(builder, params);
        }
    }

}
