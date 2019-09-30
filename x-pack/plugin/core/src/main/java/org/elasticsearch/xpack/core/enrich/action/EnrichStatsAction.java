/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.TaskInfo;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class EnrichStatsAction extends ActionType<EnrichStatsAction.Response> {

    public static final EnrichStatsAction INSTANCE = new EnrichStatsAction();
    public static final String NAME = "cluster:admin/xpack/enrich/stats";

    private EnrichStatsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeRequest<Request> {

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<ExecutingPolicy> executingPolicies;
        private final List<CoordinatorStats> coordinatorStats;

        public Response(List<ExecutingPolicy> executingPolicies, List<CoordinatorStats> coordinatorStats) {
            this.executingPolicies = executingPolicies;
            this.coordinatorStats = coordinatorStats;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            executingPolicies = in.readList(ExecutingPolicy::new);
            coordinatorStats = in.readList(CoordinatorStats::new);
        }

        public List<ExecutingPolicy> getExecutingPolicies() {
            return executingPolicies;
        }

        public List<CoordinatorStats> getCoordinatorStats() {
            return coordinatorStats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(executingPolicies);
            out.writeList(coordinatorStats);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("executing_policies");
            for (ExecutingPolicy policy : executingPolicies) {
                builder.startObject();
                policy.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            builder.startArray("coordinator_stats");
            for (CoordinatorStats entry : coordinatorStats) {
                builder.startObject();
                entry.toXContent(builder, params);
                builder.endObject();
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
            return executingPolicies.equals(response.executingPolicies) &&
                coordinatorStats.equals(response.coordinatorStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(executingPolicies, coordinatorStats);
        }

        public static class CoordinatorStats implements Writeable, ToXContentFragment {

            private final String nodeId;
            private final int queueSize;
            private final int remoteRequestsCurrent;
            private final long remoteRequestsTotal;
            private final long executedSearchesTotal;

            public CoordinatorStats(String nodeId,
                                    int queueSize,
                                    int remoteRequestsCurrent,
                                    long remoteRequestsTotal,
                                    long executedSearchesTotal) {
                this.nodeId = nodeId;
                this.queueSize = queueSize;
                this.remoteRequestsCurrent = remoteRequestsCurrent;
                this.remoteRequestsTotal = remoteRequestsTotal;
                this.executedSearchesTotal = executedSearchesTotal;
            }

            public CoordinatorStats(StreamInput in) throws IOException {
                this(in.readString(), in.readVInt(), in.readVInt(), in.readVLong(), in.readVLong());
            }

            public String getNodeId() {
                return nodeId;
            }

            public int getQueueSize() {
                return queueSize;
            }

            public int getRemoteRequestsCurrent() {
                return remoteRequestsCurrent;
            }

            public long getRemoteRequestsTotal() {
                return remoteRequestsTotal;
            }

            public long getExecutedSearchesTotal() {
                return executedSearchesTotal;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(nodeId);
                out.writeVInt(queueSize);
                out.writeVInt(remoteRequestsCurrent);
                out.writeVLong(remoteRequestsTotal);
                out.writeVLong(executedSearchesTotal);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field("node_id", nodeId);
                builder.field("queue_size", queueSize);
                builder.field("remote_requests_current", remoteRequestsCurrent);
                builder.field("remote_requests_total", remoteRequestsTotal);
                builder.field("executed_searches_total", executedSearchesTotal);
                return builder;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                CoordinatorStats stats = (CoordinatorStats) o;
                return Objects.equals(nodeId, stats.nodeId) &&
                    queueSize == stats.queueSize &&
                    remoteRequestsCurrent == stats.remoteRequestsCurrent &&
                    remoteRequestsTotal == stats.remoteRequestsTotal &&
                    executedSearchesTotal == stats.executedSearchesTotal;
            }

            @Override
            public int hashCode() {
                return Objects.hash(nodeId, queueSize, remoteRequestsCurrent, remoteRequestsTotal, executedSearchesTotal);
            }
        }

        public static class ExecutingPolicy implements Writeable, ToXContentFragment {

            private final String name;
            private final TaskInfo taskInfo;

            public ExecutingPolicy(String name, TaskInfo taskInfo) {
                this.name = name;
                this.taskInfo = taskInfo;
            }

            ExecutingPolicy(StreamInput in) throws IOException {
                this(in.readString(), new TaskInfo(in));
            }

            public String getName() {
                return name;
            }

            public TaskInfo getTaskInfo() {
                return taskInfo;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(name);
                taskInfo.writeTo(out);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field("name", name);
                builder.startObject("task");
                {
                    builder.value(taskInfo);
                }
                builder.endObject();
                return builder;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                ExecutingPolicy that = (ExecutingPolicy) o;
                return name.equals(that.name) &&
                    taskInfo.equals(that.taskInfo);
            }

            @Override
            public int hashCode() {
                return Objects.hash(name, taskInfo);
            }
        }
    }

}
