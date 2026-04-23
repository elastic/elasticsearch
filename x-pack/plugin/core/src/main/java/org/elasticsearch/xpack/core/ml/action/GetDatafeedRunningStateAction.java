/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.datafeed.CrossClusterSearchStatsSnapshot;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * Internal only action to get the current running state of a datafeed
 */
public class GetDatafeedRunningStateAction extends ActionType<GetDatafeedRunningStateAction.Response> {

    public static final GetDatafeedRunningStateAction INSTANCE = new GetDatafeedRunningStateAction();
    public static final String NAME = "cluster:internal/xpack/ml/datafeed/running_state";

    private GetDatafeedRunningStateAction() {
        super(NAME);
    }

    public static class Request extends BaseTasksRequest<Request> {

        private final Set<String> datafeedTaskIds;

        public Request(List<String> datafeedIds) {
            this.datafeedTaskIds = datafeedIds.stream().map(MlTasks::datafeedTaskId).collect(Collectors.toSet());
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.datafeedTaskIds = in.readCollectionAsSet(StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringCollection(datafeedTaskIds);
        }

        public Set<String> getDatafeedTaskIds() {
            return datafeedTaskIds;
        }

        @Override
        public boolean match(Task task) {
            return task instanceof StartDatafeedAction.DatafeedTaskMatcher && datafeedTaskIds.contains(task.getDescription());
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("get_datafeed_running_state[%s]", datafeedTaskIds), parentTaskId, headers);
        }
    }

    public static class Response extends BaseTasksResponse {

        /**
         * Snapshot of failure counters from a running datafeed's {@code ProblemTracker}.
         * Used by {@code DatafeedHealthChecker} to compute the datafeed {@code health} field
         * without requiring an additional round-trip to the ML node.
         */
        public static class DatafeedProblemStats implements Writeable {

            private final int extractionFailureCount;
            @Nullable
            private final Instant extractionFailureFirstTime;
            private final int analysisFailureCount;
            @Nullable
            private final Instant analysisFailureFirstTime;
            private final int emptyDataCount;
            private final boolean analysisFailureFatal;
            private final int delayedDataBucketCount;

            public DatafeedProblemStats(
                int extractionFailureCount,
                @Nullable Instant extractionFailureFirstTime,
                int analysisFailureCount,
                @Nullable Instant analysisFailureFirstTime,
                int emptyDataCount,
                boolean analysisFailureFatal,
                int delayedDataBucketCount
            ) {
                this.extractionFailureCount = extractionFailureCount;
                this.extractionFailureFirstTime = extractionFailureFirstTime;
                this.analysisFailureCount = analysisFailureCount;
                this.analysisFailureFirstTime = analysisFailureFirstTime;
                this.emptyDataCount = emptyDataCount;
                this.analysisFailureFatal = analysisFailureFatal;
                this.delayedDataBucketCount = delayedDataBucketCount;
            }

            public DatafeedProblemStats(StreamInput in) throws IOException {
                this.extractionFailureCount = in.readVInt();
                this.extractionFailureFirstTime = in.readOptionalInstant();
                this.analysisFailureCount = in.readVInt();
                this.analysisFailureFirstTime = in.readOptionalInstant();
                this.emptyDataCount = in.readVInt();
                this.analysisFailureFatal = in.readBoolean();
                this.delayedDataBucketCount = in.readVInt();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeVInt(extractionFailureCount);
                out.writeOptionalInstant(extractionFailureFirstTime);
                out.writeVInt(analysisFailureCount);
                out.writeOptionalInstant(analysisFailureFirstTime);
                out.writeVInt(emptyDataCount);
                out.writeBoolean(analysisFailureFatal);
                out.writeVInt(delayedDataBucketCount);
            }

            public int getExtractionFailureCount() {
                return extractionFailureCount;
            }

            @Nullable
            public Instant getExtractionFailureFirstTime() {
                return extractionFailureFirstTime;
            }

            public int getAnalysisFailureCount() {
                return analysisFailureCount;
            }

            @Nullable
            public Instant getAnalysisFailureFirstTime() {
                return analysisFailureFirstTime;
            }

            public int getEmptyDataCount() {
                return emptyDataCount;
            }

            public boolean isAnalysisFailureFatal() {
                return analysisFailureFatal;
            }

            public int getDelayedDataBucketCount() {
                return delayedDataBucketCount;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                DatafeedProblemStats that = (DatafeedProblemStats) o;
                return extractionFailureCount == that.extractionFailureCount
                    && analysisFailureCount == that.analysisFailureCount
                    && emptyDataCount == that.emptyDataCount
                    && analysisFailureFatal == that.analysisFailureFatal
                    && delayedDataBucketCount == that.delayedDataBucketCount
                    && Objects.equals(extractionFailureFirstTime, that.extractionFailureFirstTime)
                    && Objects.equals(analysisFailureFirstTime, that.analysisFailureFirstTime);
            }

            @Override
            public int hashCode() {
                return Objects.hash(
                    extractionFailureCount,
                    extractionFailureFirstTime,
                    analysisFailureCount,
                    analysisFailureFirstTime,
                    emptyDataCount,
                    analysisFailureFatal,
                    delayedDataBucketCount
                );
            }
        }

        public static class RunningState implements Writeable, ToXContentObject {

            private static final TransportVersion CROSS_CLUSTER_STATS_ADDED = TransportVersion.fromName("ml_datafeed_cross_cluster_stats");
            private static final TransportVersion DATAFEED_RUNNING_STATE_PROBLEM_STATS = TransportVersion.fromName(
                "ml_datafeed_running_state_problem_stats"
            );

            private final boolean realTimeConfigured;
            private final boolean realTimeRunning;

            @Nullable
            private final SearchInterval searchInterval;

            @Nullable
            private final CrossClusterSearchStatsSnapshot crossClusterStats;

            @Nullable
            private final DatafeedProblemStats problemStats;

            public RunningState(boolean realTimeConfigured, boolean realTimeRunning, @Nullable SearchInterval searchInterval) {
                this(realTimeConfigured, realTimeRunning, searchInterval, null, null);
            }

            public RunningState(
                boolean realTimeConfigured,
                boolean realTimeRunning,
                @Nullable SearchInterval searchInterval,
                @Nullable CrossClusterSearchStatsSnapshot crossClusterStats,
                @Nullable DatafeedProblemStats problemStats
            ) {
                this.realTimeConfigured = realTimeConfigured;
                this.realTimeRunning = realTimeRunning;
                this.searchInterval = searchInterval;
                this.crossClusterStats = crossClusterStats;
                this.problemStats = problemStats;
            }

            public RunningState(StreamInput in) throws IOException {
                this.realTimeConfigured = in.readBoolean();
                this.realTimeRunning = in.readBoolean();
                this.searchInterval = in.readOptionalWriteable(SearchInterval::new);
                if (in.getTransportVersion().supports(CROSS_CLUSTER_STATS_ADDED)) {
                    this.crossClusterStats = in.readOptionalWriteable(CrossClusterSearchStatsSnapshot::new);
                } else {
                    this.crossClusterStats = null;
                }
                if (in.getTransportVersion().supports(DATAFEED_RUNNING_STATE_PROBLEM_STATS)) {
                    this.problemStats = in.readOptionalWriteable(DatafeedProblemStats::new);
                } else {
                    this.problemStats = null;
                }
            }

            @Nullable
            public CrossClusterSearchStatsSnapshot getCrossClusterStats() {
                return crossClusterStats;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                RunningState that = (RunningState) o;
                return realTimeConfigured == that.realTimeConfigured
                    && realTimeRunning == that.realTimeRunning
                    && Objects.equals(searchInterval, that.searchInterval)
                    && Objects.equals(crossClusterStats, that.crossClusterStats)
                    && Objects.equals(problemStats, that.problemStats);
            }

            @Override
            public int hashCode() {
                return Objects.hash(realTimeConfigured, realTimeRunning, searchInterval, crossClusterStats, problemStats);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeBoolean(realTimeConfigured);
                out.writeBoolean(realTimeRunning);
                out.writeOptionalWriteable(searchInterval);
                if (out.getTransportVersion().supports(CROSS_CLUSTER_STATS_ADDED)) {
                    out.writeOptionalWriteable(crossClusterStats);
                }
                if (out.getTransportVersion().supports(DATAFEED_RUNNING_STATE_PROBLEM_STATS)) {
                    out.writeOptionalWriteable(problemStats);
                }
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("real_time_configured", realTimeConfigured);
                builder.field("real_time_running", realTimeRunning);
                if (searchInterval != null) {
                    builder.field("search_interval", searchInterval);
                }
                builder.endObject();
                return builder;
            }

            public boolean isRealTimeConfigured() {
                return realTimeConfigured;
            }

            public boolean isRealTimeRunning() {
                return realTimeRunning;
            }

            @Nullable
            public SearchInterval getSearchInterval() {
                return searchInterval;
            }

            @Nullable
            public DatafeedProblemStats getProblemStats() {
                return problemStats;
            }
        }

        private final Map<String, RunningState> datafeedRunningState;

        private static RunningState selectMostRecentState(RunningState state1, RunningState state2) {

            if (state1.searchInterval != null && state2.searchInterval != null) {
                return state1.searchInterval.startMs() > state2.searchInterval.startMs() ? state1 : state2;
            }

            if (state1.searchInterval != null) {
                return state1;
            }
            if (state2.searchInterval != null) {
                return state2;
            }

            return state2;
        }

        public static Response fromResponses(List<Response> responses) {
            return new Response(
                responses.stream()
                    .flatMap(r -> r.datafeedRunningState.entrySet().stream())
                    .filter(entry -> entry.getValue() != null)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Response::selectMostRecentState))
            );
        }

        public static Response fromTaskAndState(String datafeedId, RunningState runningState) {
            return new Response(Map.of(datafeedId, runningState));
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            datafeedRunningState = in.readMap(RunningState::new);
        }

        public Response(Map<String, RunningState> runtimeStateMap) {
            super(null, null);
            this.datafeedRunningState = runtimeStateMap;
        }

        public Optional<RunningState> getRunningState(String datafeedId) {
            return Optional.ofNullable(datafeedRunningState.get(datafeedId));
        }

        public Map<String, RunningState> getDatafeedRunningState() {
            return datafeedRunningState;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(datafeedRunningState, StreamOutput::writeWriteable);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(this.datafeedRunningState, response.datafeedRunningState);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedRunningState);
        }
    }

}
