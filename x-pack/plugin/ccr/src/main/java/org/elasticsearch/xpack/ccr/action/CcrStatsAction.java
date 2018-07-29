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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
            // sort by index name, then shard ID
            final Map<String, Map<Integer, TaskResponse>> taskResponsesByIndex = new TreeMap<>();
            for (final TaskResponse taskResponse : taskResponses) {
                taskResponsesByIndex.computeIfAbsent(
                        taskResponse.followerShardId().getIndexName(),
                        k -> new TreeMap<>()).put(taskResponse.followerShardId().getId(), taskResponse);
            }
            builder.startObject();
            {
                for (final Map.Entry<String, Map<Integer, TaskResponse>> index : taskResponsesByIndex.entrySet()) {
                    builder.startArray(index.getKey());
                    {
                        for (final Map.Entry<Integer, TaskResponse> shard : index.getValue().entrySet()) {
                            shard.getValue().status().toXContent(builder, params);
                        }
                    }
                    builder.endArray();
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
