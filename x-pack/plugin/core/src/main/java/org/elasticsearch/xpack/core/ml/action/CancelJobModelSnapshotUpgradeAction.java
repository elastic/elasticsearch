/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class CancelJobModelSnapshotUpgradeAction extends ActionType<CancelJobModelSnapshotUpgradeAction.Response> {

    public static final CancelJobModelSnapshotUpgradeAction INSTANCE = new CancelJobModelSnapshotUpgradeAction();

    // Even though at the time of writing this action doesn't have a REST endpoint the action name is
    // still "admin" rather than "internal". This is because there's no conceptual reason why this
    // action couldn't have a REST endpoint in the future, and it's painful to change these action
    // names after release. The only difference is that in 7.17 the last remaining transport client
    // users will be able to call this endpoint. In 8.x there is no transport client, so in 8.x there
    // is no difference between having "admin" and "internal" here in the period before a REST endpoint
    // exists. Using "admin" just makes life easier if we ever decide to add a REST endpoint in the
    // future.
    public static final String NAME = "cluster:admin/xpack/ml/job/model_snapshots/upgrade/cancel";

    private CancelJobModelSnapshotUpgradeAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        public static final String ALL = "_all";

        public static final ParseField SNAPSHOT_ID = new ParseField("snapshot_id");
        public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");

        static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString(Request::setJobId, Job.ID);
            PARSER.declareString(Request::setSnapshotId, SNAPSHOT_ID);
            PARSER.declareBoolean(Request::setAllowNoMatch, ALLOW_NO_MATCH);
        }

        private String jobId = ALL;
        private String snapshotId = ALL;
        private boolean allowNoMatch = true;

        public Request() {}

        public Request(String jobId, String snapshotId) {
            setJobId(jobId);
            setSnapshotId(snapshotId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            snapshotId = in.readString();
            allowNoMatch = in.readBoolean();
        }

        public final Request setJobId(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID);
            return this;
        }

        public String getJobId() {
            return jobId;
        }

        public final Request setSnapshotId(String snapshotId) {
            this.snapshotId = ExceptionsHelper.requireNonNull(snapshotId, Job.ID);
            return this;
        }

        public String getSnapshotId() {
            return snapshotId;
        }

        public boolean allowNoMatch() {
            return allowNoMatch;
        }

        public Request setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
            return this;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeString(snapshotId);
            out.writeBoolean(allowNoMatch);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field(Job.ID.getPreferredName(), jobId)
                .field(SNAPSHOT_ID.getPreferredName(), snapshotId)
                .field(ALLOW_NO_MATCH.getPreferredName(), allowNoMatch)
                .endObject();
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, snapshotId, allowNoMatch);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(jobId, other.jobId) && Objects.equals(snapshotId, other.snapshotId) && allowNoMatch == other.allowNoMatch;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static class Response extends ActionResponse implements Writeable, ToXContentObject {

        private final boolean cancelled;

        public Response(boolean cancelled) {
            this.cancelled = cancelled;
        }

        public Response(StreamInput in) throws IOException {
            cancelled = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(cancelled);
        }

        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("cancelled", cancelled);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return cancelled == response.cancelled;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cancelled);
        }
    }
}
