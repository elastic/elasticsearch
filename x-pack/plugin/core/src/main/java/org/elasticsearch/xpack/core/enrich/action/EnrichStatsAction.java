/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.Version;
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
    public static final String NAME = "cluster:monitor/xpack/enrich/stats";

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
        private final List<CacheStats> cacheStats;

        public Response(List<ExecutingPolicy> executingPolicies, List<CoordinatorStats> coordinatorStats, List<CacheStats> cacheStats) {
            this.executingPolicies = executingPolicies;
            this.coordinatorStats = coordinatorStats;
            this.cacheStats = cacheStats;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            executingPolicies = in.readList(ExecutingPolicy::new);
            coordinatorStats = in.readList(CoordinatorStats::new);
            cacheStats = in.getVersion().onOrAfter(Version.V_8_0_0) ? in.readList(CacheStats::new) : null;
        }

        public List<ExecutingPolicy> getExecutingPolicies() {
            return executingPolicies;
        }

        public List<CoordinatorStats> getCoordinatorStats() {
            return coordinatorStats;
        }

        public List<CacheStats> getCacheStats() {
            return cacheStats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(executingPolicies);
            out.writeList(coordinatorStats);
            if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
                out.writeList(cacheStats);
            }
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
            builder.startArray("cache_stats");
            for (CacheStats cacheStat : cacheStats) {
                builder.startObject();
                cacheStat.toXContent(builder, params);
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
                coordinatorStats.equals(response.coordinatorStats) &&
                cacheStats.equals(response.cacheStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(executingPolicies, coordinatorStats, cacheStats);
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

        public static class CacheStats implements Writeable, ToXContentFragment {

            private final String nodeId;
            private final long count;
            private final long hits;
            private final long misses;
            private final long evictions;

            public CacheStats(String nodeId, long count, long hits, long misses, long evictions) {
                this.nodeId = nodeId;
                this.count = count;
                this.hits = hits;
                this.misses = misses;
                this.evictions = evictions;
            }

            public CacheStats(StreamInput in) throws IOException {
                this(in.readString(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
            }

            public String getNodeId() {
                return nodeId;
            }

            public long getCount() {
                return count;
            }

            public long getHits() {
                return hits;
            }

            public long getMisses() {
                return misses;
            }

            public long getEvictions() {
                return evictions;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field("node_id", nodeId);
                builder.field("count", count);
                builder.field("hits", hits);
                builder.field("misses", misses);
                builder.field("evictions", evictions);
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(nodeId);
                out.writeVLong(count);
                out.writeVLong(hits);
                out.writeVLong(misses);
                out.writeVLong(evictions);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                CacheStats that = (CacheStats) o;
                return count == that.count && hits == that.hits && misses == that.misses && evictions == that.evictions &&
                    nodeId.equals(that.nodeId);
            }

            @Override
            public int hashCode() {
                return Objects.hash(nodeId, count, hits, misses, evictions);
            }
        }
    }

}
