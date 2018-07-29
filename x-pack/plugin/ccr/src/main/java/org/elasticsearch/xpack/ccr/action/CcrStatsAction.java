/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class CcrStatsAction extends Action<CcrStatsAction.TasksResponse> {

    public static final String NAME = "cluster:monitor/ccr/stats";

    public static final CcrStatsAction INSTANCE = new CcrStatsAction();

    private CcrStatsAction() {
        super(NAME);
    }

    @Override
    public TasksResponse newResponse() {
        return null;
    }

    public static class TasksResponse extends BaseTasksResponse implements ToXContentObject {

        private final List<TaskResponse> taskResponses;

        public TasksResponse() {
            this(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        }

        TasksResponse(
                final List<TaskOperationFailure> taskFailures,
                final List<? extends FailedNodeException> nodeFailures,
                final List<TaskResponse> taskResponses) {
            super(taskFailures, nodeFailures);
            this.taskResponses = taskResponses;
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            final Map<String, Map<Integer, TaskResponse>> taskResponsesByIndex = new TreeMap<>();
            for (final TaskResponse taskResponse : taskResponses) {
                taskResponsesByIndex.computeIfAbsent(
                        taskResponse.followerShardId().getIndexName(),
                        k -> new TreeMap<>()).put(taskResponse.followerShardId().getId(), taskResponse);
            }
            builder.startObject();
            {
                for (final Map.Entry<String, Map<Integer, TaskResponse>> index : taskResponsesByIndex.entrySet()) {
                    builder.startObject(index.getKey());
                    {
                        for (final Map.Entry<Integer, TaskResponse> shard : index.getValue().entrySet()) {
                            builder.startObject(Integer.toString(shard.getKey()));
                            {
                                final ShardFollowNodeTask.Status status = shard.getValue().status();
                                builder.field("leader_global_checkpoint", status.leaderGlobalCheckpoint());
                                builder.field("leader_max_seq_no", status.leaderMaxSeqNo());
                                builder.field("follower_global_checkpoint", status.followerGlobalCheckpoint());
                                builder.field("follower_max_seq_no", status.followerMaxSeqNo());
                                builder.field("last_requested_seq_no", status.lastRequestedSeqNo());
                                builder.field("number_of_concurrent_reads", status.numberOfConcurrentReads());
                                builder.field("number_of_concurrent_writes", status.numberOfConcurrentWrites());
                                builder.field("number_of_queued_writes", status.numberOfQueuedWrites());
                                builder.humanReadableField(
                                        "total_fetch_time_millis",
                                        "total_fetch_time",
                                        new TimeValue(status.totalFetchTimeNanos(), TimeUnit.NANOSECONDS));
                                builder.field("number_of_successful_fetches", status.numberOfSuccessfulFetches());
                                builder.field("number_of_fetches", status.numberOfSuccessfulFetches() + status.numberOfFailedFetches());
                                builder.field("operations_received", status.operationsReceived());
                                builder.humanReadableField(
                                        "total_transferred_bytes",
                                        "total_transferred",
                                        new ByteSizeValue(status.totalTransferredBytes(), ByteSizeUnit.BYTES));
                                builder.humanReadableField(
                                        "total_index_time_millis",
                                        "total_index_time",
                                        new TimeValue(status.totalIndexTimeNanos(), TimeUnit.NANOSECONDS));
                                builder.field("number_of_successful_bulk_operations", status.numberOfSuccessfulBulkOperations());
                                builder.field(
                                        "number_of_bulk_operations",
                                        status.numberOfSuccessfulBulkOperations() + status.numberOfFailedBulkOperations());
                                builder.field("number_of_operations_indexed", status.numberOfOperationsIndexed());
                            }
                            builder.endObject();
                        }
                    }
                    builder.endObject();
                }
            }
            builder.endObject();
            return builder;
        }
    }

    public static class TasksRequest extends BaseTasksRequest<TasksRequest> {

        private String indexName;

        public void setIndexName(final String indexName) {
            this.indexName = indexName;
        }

        @Override
        public boolean match(final Task task) {
            if (task instanceof ShardFollowNodeTask) {
                final ShardFollowNodeTask shardTask = (ShardFollowNodeTask) task;
                return indexName.equals("") || indexName.equals("_all") || shardTask.getFollowShardId().getIndexName().equals(indexName);
            }
            return false;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            indexName = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(indexName);
        }

    }


    public static class TaskResponse implements Writeable {

        private final ShardId followerShardId;

        ShardId followerShardId() {
            return followerShardId;
        }

        private final ShardFollowNodeTask.Status status;

        ShardFollowNodeTask.Status status() {
            return status;
        }

        TaskResponse(final ShardId followerShardId, final ShardFollowNodeTask.Status status) {
            this.followerShardId = followerShardId;
            this.status = status;
        }

        TaskResponse(final StreamInput in) throws IOException {
            this.followerShardId = ShardId.readShardId(in);
            this.status = new ShardFollowNodeTask.Status(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            followerShardId.writeTo(out);
            status.writeTo(out);
        }

    }

}
