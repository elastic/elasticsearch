/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedRunningStateAction.Response.RunningState;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

public class GetDatafeedsStatsAction extends ActionType<GetDatafeedsStatsAction.Response> {

    public static final GetDatafeedsStatsAction INSTANCE = new GetDatafeedsStatsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/datafeeds/stats/get";

    public static final String ALL = "_all";
    private static final String STATE = "state";
    private static final String NODE = "node";
    private static final String ASSIGNMENT_EXPLANATION = "assignment_explanation";
    private static final String TIMING_STATS = "timing_stats";
    private static final String RUNNING_STATE = "running_state";

    private GetDatafeedsStatsAction() {
        super(NAME, Response::new);
    }

    // This needs to be a MasterNodeReadRequest even though the corresponding transport
    // action is a HandledTransportAction so that in mixed version clusters it can be
    // serialized to older nodes where the transport action was a MasterNodeReadAction.
    // TODO: Make this a simple request in a future version where there is no possibility
    // of this request being serialized to another node.
    public static class Request extends MasterNodeReadRequest<Request> {

        public static final String ALLOW_NO_MATCH = "allow_no_match";

        private String datafeedId;
        private boolean allowNoMatch = true;

        public Request(String datafeedId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            datafeedId = in.readString();
            allowNoMatch = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            out.writeBoolean(allowNoMatch);
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public boolean allowNoMatch() {
            return allowNoMatch;
        }

        public void setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, allowNoMatch);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(datafeedId, other.datafeedId) && Objects.equals(allowNoMatch, other.allowNoMatch);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("get_datafeed_stats[%s]", datafeedId), parentTaskId, headers);
        }
    }

    public static class Response extends AbstractGetResourcesResponse<Response.DatafeedStats> implements ToXContentObject {

        public static class DatafeedStats implements ToXContentObject, Writeable {

            private final String datafeedId;
            private final DatafeedState datafeedState;
            @Nullable
            private final DiscoveryNode node;
            @Nullable
            private final String assignmentExplanation;
            @Nullable
            private final DatafeedTimingStats timingStats;
            @Nullable
            private final RunningState runningState;

            public DatafeedStats(
                String datafeedId,
                DatafeedState datafeedState,
                @Nullable DiscoveryNode node,
                @Nullable String assignmentExplanation,
                @Nullable DatafeedTimingStats timingStats,
                @Nullable RunningState runningState
            ) {
                this.datafeedId = Objects.requireNonNull(datafeedId);
                this.datafeedState = Objects.requireNonNull(datafeedState);
                this.node = node;
                this.assignmentExplanation = assignmentExplanation;
                this.timingStats = timingStats;
                this.runningState = runningState;
            }

            DatafeedStats(StreamInput in) throws IOException {
                datafeedId = in.readString();
                datafeedState = DatafeedState.fromStream(in);
                node = in.readOptionalWriteable(DiscoveryNode::new);
                assignmentExplanation = in.readOptionalString();
                timingStats = in.readOptionalWriteable(DatafeedTimingStats::new);
                runningState = in.readOptionalWriteable(RunningState::new);
            }

            public String getDatafeedId() {
                return datafeedId;
            }

            public DatafeedState getDatafeedState() {
                return datafeedState;
            }

            public DiscoveryNode getNode() {
                return node;
            }

            public String getAssignmentExplanation() {
                return assignmentExplanation;
            }

            public DatafeedTimingStats getTimingStats() {
                return timingStats;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
                builder.field(STATE, datafeedState.toString());
                if (node != null) {
                    builder.startObject(NODE);
                    builder.field("id", node.getId());
                    builder.field("name", node.getName());
                    builder.field("ephemeral_id", node.getEphemeralId());
                    builder.field("transport_address", node.getAddress().toString());

                    builder.startObject("attributes");
                    for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
                        if (entry.getKey().startsWith("ml.")) {
                            builder.field(entry.getKey(), entry.getValue());
                        }
                    }
                    builder.endObject();
                    builder.endObject();
                }
                if (assignmentExplanation != null) {
                    builder.field(ASSIGNMENT_EXPLANATION, assignmentExplanation);
                }
                if (timingStats != null) {
                    builder.field(
                        TIMING_STATS,
                        timingStats,
                        new MapParams(Collections.singletonMap(ToXContentParams.INCLUDE_CALCULATED_FIELDS, "true"))
                    );
                }
                if (runningState != null) {
                    builder.field(RUNNING_STATE, runningState);
                }
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(datafeedId);
                datafeedState.writeTo(out);
                out.writeOptionalWriteable(node);
                out.writeOptionalString(assignmentExplanation);
                out.writeOptionalWriteable(timingStats);
                out.writeOptionalWriteable(runningState);
            }

            @Override
            public int hashCode() {
                return Objects.hash(datafeedId, datafeedState, node, assignmentExplanation, timingStats, runningState);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                DatafeedStats other = (DatafeedStats) obj;
                return Objects.equals(this.datafeedId, other.datafeedId)
                    && Objects.equals(this.datafeedState, other.datafeedState)
                    && Objects.equals(this.node, other.node)
                    && Objects.equals(this.assignmentExplanation, other.assignmentExplanation)
                    && Objects.equals(this.runningState, other.runningState)
                    && Objects.equals(this.timingStats, other.timingStats);
            }

            public static Builder builder(String datafeedId) {
                return new Builder(datafeedId);
            }

            private static class Builder {
                private final String datafeedId;
                private DatafeedState datafeedState;
                private DiscoveryNode node;
                private String assignmentExplanation;
                private DatafeedTimingStats timingStats;
                private RunningState runningState;

                private Builder(String datafeedId) {
                    this.datafeedId = datafeedId;
                }

                private String getDatafeedId() {
                    return datafeedId;
                }

                private Builder setDatafeedState(DatafeedState datafeedState) {
                    this.datafeedState = datafeedState;
                    return this;
                }

                private Builder setNode(DiscoveryNode node) {
                    this.node = node;
                    return this;
                }

                private Builder setAssignmentExplanation(String assignmentExplanation) {
                    this.assignmentExplanation = assignmentExplanation;
                    return this;
                }

                private Builder setTimingStats(DatafeedTimingStats timingStats) {
                    this.timingStats = timingStats;
                    return this;
                }

                private Builder setRunningState(RunningState runningState) {
                    this.runningState = runningState;
                    return this;
                }

                private DatafeedStats build() {
                    return new DatafeedStats(datafeedId, datafeedState, node, assignmentExplanation, timingStats, runningState);
                }
            }
        }

        public Response(QueryPage<DatafeedStats> datafeedsStats) {
            super(datafeedsStats);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public QueryPage<DatafeedStats> getResponse() {
            return getResources();
        }

        @Override
        protected Reader<DatafeedStats> getReader() {
            return DatafeedStats::new;
        }

        public static class Builder {
            private List<DatafeedStats.Builder> statsBuilders;
            private Map<String, String> datafeedToJobId;
            private Map<String, DatafeedTimingStats> timingStatsMap;
            private GetDatafeedRunningStateAction.Response datafeedRuntimeState;

            public Builder setDatafeedIds(Collection<String> datafeedIds) {
                this.statsBuilders = datafeedIds.stream()
                    .map(GetDatafeedsStatsAction.Response.DatafeedStats::builder)
                    .collect(Collectors.toList());
                return this;
            }

            public List<String> getDatafeedIds() {
                return statsBuilders.stream().map(DatafeedStats.Builder::getDatafeedId).collect(Collectors.toList());
            }

            public Builder setDatafeedToJobId(Map<String, String> datafeedToJobId) {
                this.datafeedToJobId = datafeedToJobId;
                return this;
            }

            public Builder setTimingStatsMap(Map<String, DatafeedTimingStats> timingStatsMap) {
                this.timingStatsMap = timingStatsMap;
                return this;
            }

            public Builder setDatafeedRuntimeState(GetDatafeedRunningStateAction.Response datafeedRuntimeState) {
                this.datafeedRuntimeState = datafeedRuntimeState;
                return this;
            }

            public Response build(PersistentTasksCustomMetadata tasksInProgress, ClusterState state) {
                List<DatafeedStats> stats = statsBuilders.stream().map(statsBuilder -> {
                    final String jobId = datafeedToJobId.get(statsBuilder.datafeedId);
                    DatafeedTimingStats timingStats = jobId == null
                        ? null
                        : timingStatsMap.getOrDefault(jobId, new DatafeedTimingStats(jobId));
                    PersistentTasksCustomMetadata.PersistentTask<?> maybeTask = MlTasks.getDatafeedTask(
                        statsBuilder.datafeedId,
                        tasksInProgress
                    );
                    DatafeedState datafeedState = MlTasks.getDatafeedState(statsBuilder.datafeedId, tasksInProgress);
                    DiscoveryNode node = null;
                    if (maybeTask != null && maybeTask.getExecutorNode() != null) {
                        node = state.getNodes().get(maybeTask.getExecutorNode());
                    }
                    return statsBuilder.setNode(node)
                        .setDatafeedState(datafeedState)
                        .setAssignmentExplanation(maybeTask != null ? maybeTask.getAssignment().getExplanation() : null)
                        .setTimingStats(timingStats)
                        .setRunningState(datafeedRuntimeState.getRunningState(statsBuilder.datafeedId).orElse(null))
                        .build();
                }).collect(Collectors.toList());
                return new Response(new QueryPage<>(stats, stats.size(), DatafeedConfig.RESULTS_FIELD));
            }
        }
    }
}
