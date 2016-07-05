/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cockroach;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A cluster state record that contains a list of all running cockroach tasks
 */
public class CockroachTasksInProgress extends AbstractDiffable<ClusterState.Custom> implements ClusterState.Custom {
    public static final String TYPE = "cockroach_tasks";

    public static final CockroachTasksInProgress PROTO = new CockroachTasksInProgress();

    // TODO: Implement custom Diff for entries
    private final List<CockroachTaskInProgress> entries;

    public CockroachTasksInProgress(List<CockroachTaskInProgress> entries) {
        this.entries = entries;
    }

    public CockroachTasksInProgress(CockroachTaskInProgress... entries) {
        this.entries = Arrays.asList(entries);
    }

    public List<CockroachTaskInProgress> entries() {
        return this.entries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CockroachTasksInProgress that = (CockroachTasksInProgress) o;

        if (!entries.equals(that.entries)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    /**
     * A record that represents a single running cockroach task
     */
    public static class CockroachTaskInProgress<Request extends CockroachRequest<Request>, Response extends CockroachResponse> {
        private final String uuid;
        private final String action;
        private final Request request;
        @Nullable
        private final Response response;
        @Nullable
        private final Exception failure;
        @Nullable
        private final String executorNode;
        private final String callerNode;
        private final TaskId callerTaskId;


        public CockroachTaskInProgress(String uuid, String action, Request request, Response response, Exception failure,
                                       String executorNode, String callerNode, TaskId callerTaskId) {
            this.uuid = uuid;
            this.action = action;
            this.request = request;
            this.response = response;
            this.failure = failure;
            this.executorNode = executorNode;
            this.callerNode = callerNode;
            this.callerTaskId = callerTaskId;
        }

        public CockroachTaskInProgress(String uuid, String action, Request request, String executorNode, String callerNode,
                                       TaskId callerTaskId) {
            this(uuid, action, request, null, null, executorNode, callerNode, callerTaskId);
        }


        public CockroachTaskInProgress(CockroachTaskInProgress<Request, Response> task, Response response, Exception failure) {
            this(task.uuid, task.action, task.request, response, failure, task.executorNode, task.callerNode, task.callerTaskId);
        }

        public CockroachTaskInProgress(CockroachTaskInProgress<Request, Response> task, String executorNode, String callerNode) {
            this(task.uuid, task.action, task.request, task.response, task.failure, executorNode, callerNode, task.callerTaskId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CockroachTaskInProgress<?, ?> that = (CockroachTaskInProgress<?, ?>) o;
            return Objects.equals(uuid, that.uuid) &&
                Objects.equals(action, that.action) &&
                Objects.equals(request, that.request) &&
                Objects.equals(response, that.response) &&
                Objects.equals(failure, that.failure) &&
                Objects.equals(executorNode, that.executorNode) &&
                Objects.equals(callerNode, that.callerNode) &&
                Objects.equals(callerTaskId, that.callerTaskId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uuid, action, request, response, failure, executorNode, callerNode, callerTaskId);
        }

        public String getUuid() {

            return uuid;
        }

        public String getAction() {
            return action;
        }

        public Request getRequest() {
            return request;
        }

        public Response getResponse() {
            return response;
        }

        public Exception getFailure() {
            return failure;
        }

        public String getExecutorNode() {
            return executorNode;
        }

        public String getCallerNode() {
            return callerNode;
        }

        public TaskId getCallerTaskId() {
            return callerTaskId;
        }

        public boolean isFinished() {
            return response != null || failure != null;
        }
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public CockroachTasksInProgress readFrom(StreamInput in) throws IOException {
        CockroachTaskInProgress[] entries = new CockroachTaskInProgress[in.readVInt()];
        for (int i = 0; i < entries.length; i++) {
            final String uuid = in.readString();
            final String action = in.readString();
            final CockroachRequest request = in.readNamedWriteable(CockroachRequest.class);
            final CockroachResponse response = in.readOptionalNamedWriteable(CockroachResponse.class);
            final Exception failure = in.readException();
            final String executorNode = in.readString();
            final String callerNode = in.readString();
            final TaskId callerTaskId = TaskId.readFromStream(in);
            entries[i] = new CockroachTaskInProgress(uuid, action, request, response, failure, executorNode, callerNode, callerTaskId);
        }
        return new CockroachTasksInProgress(entries);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(entries.size());
        for (CockroachTaskInProgress entry : entries) {
            out.writeString(entry.uuid);
            out.writeString(entry.action);
            out.writeNamedWriteable(entry.request);
            out.writeOptionalNamedWriteable(entry.response);
            out.writeException(entry.failure);
            out.writeString(entry.executorNode);
            out.writeString(entry.callerNode);
            entry.callerTaskId.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray("running_tasks");
        for (CockroachTaskInProgress entry : entries) {
            toXContent(entry, builder, params);
        }
        builder.endArray();
        return builder;
    }

    public void toXContent(CockroachTaskInProgress entry, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        {
            builder.field("uuid", entry.uuid);
            builder.field("action", entry.action);
            builder.field("request");
            entry.request.toXContent(builder, params);
            if (entry.response != null) {
                builder.field("response");
                entry.response.toXContent(builder, params);
            }
            if (entry.failure != null) {
                builder.startObject("failure");
                ElasticsearchException.toXContent(builder, params, entry.failure);
                builder.endObject();
            }
            builder.field("executor_node", entry.executorNode);
            builder.field("caller_node", entry.callerNode);
            builder.field("caller_task_id", entry.callerTaskId.toString());
        }
        builder.endObject();
    }
}
