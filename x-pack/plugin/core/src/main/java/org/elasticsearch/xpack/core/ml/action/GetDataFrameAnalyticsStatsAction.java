/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetDataFrameAnalyticsStatsAction extends ActionType<GetDataFrameAnalyticsStatsAction.Response> {

    public static final GetDataFrameAnalyticsStatsAction INSTANCE = new GetDataFrameAnalyticsStatsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/data_frame/analytics/stats/get";

    private GetDataFrameAnalyticsStatsAction() {
        super(NAME, GetDataFrameAnalyticsStatsAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> {

        public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");

        private String id;
        private boolean allowNoMatch = true;
        private PageParams pageParams = PageParams.defaultParams();

        // Used internally to store the expanded IDs
        private List<String> expandedIds = Collections.emptyList();

        public Request(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, DataFrameAnalyticsConfig.ID.getPreferredName());
            this.expandedIds = Collections.singletonList(id);
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            allowNoMatch = in.readBoolean();
            pageParams = in.readOptionalWriteable(PageParams::new);
            expandedIds = in.readStringList();
        }

        public void setExpandedIds(List<String> expandedIds) {
            this.expandedIds = Objects.requireNonNull(expandedIds);
        }

        public List<String> getExpandedIds() {
            return expandedIds;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeBoolean(allowNoMatch);
            out.writeOptionalWriteable(pageParams);
            out.writeStringCollection(expandedIds);
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        public boolean isAllowNoMatch() {
            return allowNoMatch;
        }

        public void setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
        }

        public void setPageParams(PageParams pageParams) {
            this.pageParams = pageParams;
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        @Override
        public boolean match(Task task) {
            return expandedIds.stream().anyMatch(expandedId -> StartDataFrameAnalyticsAction.TaskMatcher.match(task, expandedId));
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, allowNoMatch, pageParams);
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
            return Objects.equals(id, other.id) && allowNoMatch == other.allowNoMatch && Objects.equals(pageParams, other.pageParams);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        public RequestBuilder(ElasticsearchClient client, GetDataFrameAnalyticsStatsAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends BaseTasksResponse implements ToXContentObject {

        public static class Stats implements ToXContentObject, Writeable {

            private final String id;
            private final DataFrameAnalyticsState state;
            @Nullable
            private final String failureReason;

            /**
             * The progress is described as a list of each phase and its completeness percentage.
             */
            private final List<PhaseProgress> progress;

            @Nullable
            private final DiscoveryNode node;
            @Nullable
            private final String assignmentExplanation;

            public Stats(String id, DataFrameAnalyticsState state, @Nullable String failureReason, List<PhaseProgress> progress,
                         @Nullable DiscoveryNode node, @Nullable String assignmentExplanation) {
                this.id = Objects.requireNonNull(id);
                this.state = Objects.requireNonNull(state);
                this.failureReason = failureReason;
                this.progress = Objects.requireNonNull(progress);
                this.node = node;
                this.assignmentExplanation = assignmentExplanation;
            }

            public Stats(StreamInput in) throws IOException {
                id = in.readString();
                state = DataFrameAnalyticsState.fromStream(in);
                failureReason = in.readOptionalString();
                if (in.getVersion().before(Version.V_7_4_0)) {
                    progress = readProgressFromLegacy(state, in);
                } else {
                    progress = in.readList(PhaseProgress::new);
                }
                node = in.readOptionalWriteable(DiscoveryNode::new);
                assignmentExplanation = in.readOptionalString();
            }

            private static List<PhaseProgress> readProgressFromLegacy(DataFrameAnalyticsState state, StreamInput in) throws IOException {
                Integer legacyProgressPercent = in.readOptionalInt();
                if (legacyProgressPercent == null) {
                    return Collections.emptyList();
                }

                int reindexingProgress = 0;
                int loadingDataProgress = 0;
                int analyzingProgress = 0;
                switch (state) {
                    case ANALYZING:
                        reindexingProgress = 100;
                        loadingDataProgress = 100;
                        analyzingProgress = legacyProgressPercent;
                        break;
                    case REINDEXING:
                        reindexingProgress = legacyProgressPercent;
                        break;
                    case STARTING:
                    case STARTED:
                    case STOPPED:
                    case STOPPING:
                    default:
                        return null;
                }

                return Arrays.asList(
                    new PhaseProgress("reindexing", reindexingProgress),
                    new PhaseProgress("loading_data", loadingDataProgress),
                    new PhaseProgress("analyzing", analyzingProgress),
                    new PhaseProgress("writing_results", 0));
            }

            public String getId() {
                return id;
            }

            public DataFrameAnalyticsState getState() {
                return state;
            }

            public String getFailureReason() {
                return failureReason;
            }

            public List<PhaseProgress> getProgress() {
                return progress;
            }

            public DiscoveryNode getNode() {
                return node;
            }

            public String getAssignmentExplanation() {
                return assignmentExplanation;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                // TODO: Have callers wrap the content with an object as they choose rather than forcing it upon them
                builder.startObject();
                {
                    toUnwrappedXContent(builder);
                }
                return builder.endObject();
            }

            public XContentBuilder toUnwrappedXContent(XContentBuilder builder) throws IOException {
                builder.field(DataFrameAnalyticsConfig.ID.getPreferredName(), id);
                builder.field("state", state.toString());
                if (failureReason != null) {
                    builder.field("failure_reason", failureReason);
                }
                if (progress != null) {
                    builder.field("progress", progress);
                }
                if (node != null) {
                    builder.startObject("node");
                    builder.field("id", node.getId());
                    builder.field("name", node.getName());
                    builder.field("ephemeral_id", node.getEphemeralId());
                    builder.field("transport_address", node.getAddress().toString());

                    builder.startObject("attributes");
                    for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
                        builder.field(entry.getKey(), entry.getValue());
                    }
                    builder.endObject();
                    builder.endObject();
                }
                if (assignmentExplanation != null) {
                    builder.field("assignment_explanation", assignmentExplanation);
                }
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(id);
                state.writeTo(out);
                out.writeOptionalString(failureReason);
                if (out.getVersion().before(Version.V_7_4_0)) {
                    writeProgressToLegacy(out);
                } else {
                    out.writeList(progress);
                }
                out.writeOptionalWriteable(node);
                out.writeOptionalString(assignmentExplanation);
            }

            private void writeProgressToLegacy(StreamOutput out) throws IOException {
                String targetPhase = null;
                switch (state) {
                    case ANALYZING:
                        targetPhase = "analyzing";
                        break;
                    case REINDEXING:
                        targetPhase = "reindexing";
                        break;
                    case STARTING:
                    case STARTED:
                    case STOPPED:
                    case STOPPING:
                    default:
                        break;
                }

                Integer legacyProgressPercent = null;
                for (PhaseProgress phaseProgress : progress) {
                    if (phaseProgress.getPhase().equals(targetPhase)) {
                        legacyProgressPercent = phaseProgress.getProgressPercent();
                    }
                }
                out.writeOptionalInt(legacyProgressPercent);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id, state, failureReason, progress, node, assignmentExplanation);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                Stats other = (Stats) obj;
                return Objects.equals(id, other.id)
                        && Objects.equals(this.state, other.state)
                        && Objects.equals(this.failureReason, other.failureReason)
                        && Objects.equals(this.progress, other.progress)
                        && Objects.equals(this.node, other.node)
                        && Objects.equals(this.assignmentExplanation, other.assignmentExplanation);
            }
        }

        private QueryPage<Stats> stats;

        public Response(QueryPage<Stats> stats) {
            this(Collections.emptyList(), Collections.emptyList(), stats);
        }

        public Response(List<TaskOperationFailure> taskFailures, List<? extends ElasticsearchException> nodeFailures,
                        QueryPage<Stats> stats) {
            super(taskFailures, nodeFailures);
            this.stats = stats;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            stats = new QueryPage<>(in, Stats::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            stats.writeTo(out);
        }

        public QueryPage<Stats> getResponse() {
            return stats;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            stats.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(stats);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(stats, other.stats);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

}
