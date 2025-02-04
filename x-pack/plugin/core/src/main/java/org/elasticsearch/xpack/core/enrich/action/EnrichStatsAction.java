/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class EnrichStatsAction extends ActionType<EnrichStatsAction.Response> {

    public static final EnrichStatsAction INSTANCE = new EnrichStatsAction();
    public static final String NAME = "cluster:monitor/xpack/enrich/stats";

    private EnrichStatsAction() {
        super(NAME);
    }

    public static class Request extends LocalClusterStateRequest {

        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        /**
         * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
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

        public List<ExecutingPolicy> getExecutingPolicies() {
            return executingPolicies;
        }

        public List<CoordinatorStats> getCoordinatorStats() {
            return coordinatorStats;
        }

        public List<CacheStats> getCacheStats() {
            return cacheStats;
        }

        /**
         * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to write these responses until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(executingPolicies);
            out.writeCollection(coordinatorStats);
            out.writeCollection(cacheStats);
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
            if (cacheStats != null) {
                builder.startArray("cache_stats");
                for (CacheStats cacheStat : cacheStats) {
                    builder.startObject();
                    cacheStat.toXContent(builder, params);
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return executingPolicies.equals(response.executingPolicies)
                && coordinatorStats.equals(response.coordinatorStats)
                && Objects.equals(cacheStats, response.cacheStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(executingPolicies, coordinatorStats, cacheStats);
        }

        public record CoordinatorStats(
            String nodeId,
            int queueSize,
            int remoteRequestsCurrent,
            long remoteRequestsTotal,
            long executedSearchesTotal
        ) implements Writeable, ToXContentFragment {

            public CoordinatorStats(StreamInput in) throws IOException {
                this(in.readString(), in.readVInt(), in.readVInt(), in.readVLong(), in.readVLong());
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
        }

        public record ExecutingPolicy(String name, TaskInfo taskInfo) implements Writeable, ToXContentFragment {

            /**
             * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to write these responses until
             * we no longer need to support calling this action remotely.
             */
            @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
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
        }

        public record CacheStats(
            String nodeId,
            long count,
            long hits,
            long misses,
            long evictions,
            long hitsTimeInMillis,
            long missesTimeInMillis,
            long cacheSizeInBytes
        ) implements Writeable, ToXContentFragment {

            public CacheStats(StreamInput in) throws IOException {
                this(
                    in.readString(),
                    in.readVLong(),
                    in.readVLong(),
                    in.readVLong(),
                    in.readVLong(),
                    in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0) ? in.readLong() : -1,
                    in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0) ? in.readLong() : -1,
                    in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0) ? in.readLong() : -1
                );
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field("node_id", nodeId);
                builder.field("count", count);
                builder.field("hits", hits);
                builder.field("misses", misses);
                builder.field("evictions", evictions);
                builder.humanReadableField("hits_time_in_millis", "hits_time", new TimeValue(hitsTimeInMillis));
                builder.humanReadableField("misses_time_in_millis", "misses_time", new TimeValue(missesTimeInMillis));
                builder.humanReadableField("size_in_bytes", "size", ByteSizeValue.ofBytes(cacheSizeInBytes));
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(nodeId);
                out.writeVLong(count);
                out.writeVLong(hits);
                out.writeVLong(misses);
                out.writeVLong(evictions);
                if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                    out.writeLong(hitsTimeInMillis);
                    out.writeLong(missesTimeInMillis);
                }
                if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                    out.writeLong(cacheSizeInBytes);
                }
            }
        }
    }

}
