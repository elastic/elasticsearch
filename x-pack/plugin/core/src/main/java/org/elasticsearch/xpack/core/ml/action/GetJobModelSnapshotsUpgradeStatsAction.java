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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ml.action.UpgradeJobModelSnapshotAction.Request.SNAPSHOT_ID;

public class GetJobModelSnapshotsUpgradeStatsAction extends ActionType<GetJobModelSnapshotsUpgradeStatsAction.Response> {

    public static final GetJobModelSnapshotsUpgradeStatsAction INSTANCE = new GetJobModelSnapshotsUpgradeStatsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/job/model_snapshots/upgrade/stats/get";

    public static final String ALL = "_all";
    private static final String STATE = "state";
    private static final String NODE = "node";
    private static final String ASSIGNMENT_EXPLANATION = "assignment_explanation";

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("model_snapshot_upgrades");
    public static String TYPE = "model_snapshot_upgrade";

    private GetJobModelSnapshotsUpgradeStatsAction() {
        super(NAME, GetJobModelSnapshotsUpgradeStatsAction.Response::new);
    }

    public static class Request extends MasterNodeReadRequest<GetJobModelSnapshotsUpgradeStatsAction.Request> {

        public static final String ALLOW_NO_MATCH = "allow_no_match";

        private final String jobId;
        private final String snapshotId;
        private boolean allowNoMatch = true;

        public Request(String jobId, String snapshotId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
            this.snapshotId = ExceptionsHelper.requireNonNull(snapshotId, SNAPSHOT_ID.getPreferredName());
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            snapshotId = in.readString();
            allowNoMatch = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeString(snapshotId);
            out.writeBoolean(allowNoMatch);
        }

        public String getJobId() {
            return jobId;
        }

        public String getSnapshotId() {
            return snapshotId;
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
            return Objects.hash(jobId, snapshotId, allowNoMatch);
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
            return Objects.equals(jobId, other.jobId)
                && Objects.equals(snapshotId, other.snapshotId)
                && Objects.equals(allowNoMatch, other.allowNoMatch);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(
                id,
                type,
                action,
                format("get_job_model_snapshot_upgrade_stats[%s:%s]", jobId, snapshotId),
                parentTaskId,
                headers
            );
        }
    }

    public static class Response extends AbstractGetResourcesResponse<Response.JobModelSnapshotUpgradeStats> implements ToXContentObject {

        public static class JobModelSnapshotUpgradeStats implements ToXContentObject, Writeable {

            private final String jobId;
            private final String snapshotId;
            private final SnapshotUpgradeState upgradeState;
            @Nullable
            private final DiscoveryNode node;
            @Nullable
            private final String assignmentExplanation;

            public JobModelSnapshotUpgradeStats(
                String jobId,
                String snapshotId,
                SnapshotUpgradeState upgradeState,
                @Nullable DiscoveryNode node,
                @Nullable String assignmentExplanation
            ) {
                this.jobId = Objects.requireNonNull(jobId);
                this.snapshotId = Objects.requireNonNull(snapshotId);
                this.upgradeState = Objects.requireNonNull(upgradeState);
                this.node = node;
                this.assignmentExplanation = assignmentExplanation;
            }

            JobModelSnapshotUpgradeStats(StreamInput in) throws IOException {
                jobId = in.readString();
                snapshotId = in.readString();
                upgradeState = SnapshotUpgradeState.fromStream(in);
                node = in.readOptionalWriteable(DiscoveryNode::new);
                assignmentExplanation = in.readOptionalString();
            }

            public String getJobId() {
                return jobId;
            }

            public String getSnapshotId() {
                return snapshotId;
            }

            public SnapshotUpgradeState getUpgradeState() {
                return upgradeState;
            }

            public DiscoveryNode getNode() {
                return node;
            }

            public String getAssignmentExplanation() {
                return assignmentExplanation;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(Job.ID.getPreferredName(), jobId);
                builder.field(SNAPSHOT_ID.getPreferredName(), snapshotId);
                builder.field(STATE, upgradeState.toString());
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
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(jobId);
                out.writeString(snapshotId);
                upgradeState.writeTo(out);
                out.writeOptionalWriteable(node);
                out.writeOptionalString(assignmentExplanation);
            }

            @Override
            public int hashCode() {
                return Objects.hash(jobId, snapshotId, upgradeState, node, assignmentExplanation);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                Response.JobModelSnapshotUpgradeStats other = (Response.JobModelSnapshotUpgradeStats) obj;
                return Objects.equals(this.jobId, other.jobId)
                    && Objects.equals(this.snapshotId, other.snapshotId)
                    && Objects.equals(this.upgradeState, other.upgradeState)
                    && Objects.equals(this.node, other.node)
                    && Objects.equals(this.assignmentExplanation, other.assignmentExplanation);
            }

            public static Response.JobModelSnapshotUpgradeStats.Builder builder(String jobId, String snapshotId) {
                return new Response.JobModelSnapshotUpgradeStats.Builder(jobId, snapshotId);
            }

            public static class Builder {
                private final String jobId;
                private final String snapshotId;
                private SnapshotUpgradeState upgradeState;
                private DiscoveryNode node;
                private String assignmentExplanation;

                public Builder(String jobId, String snapshotId) {
                    this.jobId = jobId;
                    this.snapshotId = snapshotId;
                }

                public String getJobId() {
                    return jobId;
                }

                public String getSnapshotId() {
                    return snapshotId;
                }

                public Response.JobModelSnapshotUpgradeStats.Builder setUpgradeState(SnapshotUpgradeState upgradeState) {
                    this.upgradeState = Objects.requireNonNull(upgradeState);
                    return this;
                }

                public Response.JobModelSnapshotUpgradeStats.Builder setNode(DiscoveryNode node) {
                    this.node = node;
                    return this;
                }

                public Response.JobModelSnapshotUpgradeStats.Builder setAssignmentExplanation(String assignmentExplanation) {
                    this.assignmentExplanation = assignmentExplanation;
                    return this;
                }

                public Response.JobModelSnapshotUpgradeStats build() {
                    return new Response.JobModelSnapshotUpgradeStats(jobId, snapshotId, upgradeState, node, assignmentExplanation);
                }
            }
        }

        public Response(QueryPage<Response.JobModelSnapshotUpgradeStats> upgradeStats) {
            super(upgradeStats);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public QueryPage<Response.JobModelSnapshotUpgradeStats> getResponse() {
            return getResources();
        }

        @Override
        protected Reader<Response.JobModelSnapshotUpgradeStats> getReader() {
            return Response.JobModelSnapshotUpgradeStats::new;
        }
    }
}
