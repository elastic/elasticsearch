/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class UpgradeJobModelSnapshotAction extends ActionType<UpgradeJobModelSnapshotAction.Response> {

    public static final UpgradeJobModelSnapshotAction INSTANCE = new UpgradeJobModelSnapshotAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/model_snapshots/upgrade";

    private UpgradeJobModelSnapshotAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {
        // Default to 30m as loading an older snapshot can take a while
        public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueMinutes(30);

        public static final ParseField SNAPSHOT_ID = new ParseField("snapshot_id");
        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField WAIT_FOR_COMPLETION = new ParseField("wait_for_completion");

        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            a -> new UpgradeJobModelSnapshotAction.Request((String) a[0], (String) a[1], (String) a[2], (Boolean) a[3])
        );
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_ID);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TIMEOUT);
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), WAIT_FOR_COMPLETION);
        }

        public static UpgradeJobModelSnapshotAction.Request parseRequest(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final String jobId;
        private final String snapshotId;
        private final TimeValue timeout;
        private final boolean waitForCompletion;

        Request(String jobId, String snapshotId, String timeout, Boolean waitForCompletion) {
            this(
                jobId,
                snapshotId,
                timeout == null ? null : TimeValue.parseTimeValue(timeout, TIMEOUT.getPreferredName()),
                waitForCompletion != null && waitForCompletion
            );
        }

        public Request(String jobId, String snapshotId, TimeValue timeValue, boolean waitForCompletion) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID);
            this.snapshotId = ExceptionsHelper.requireNonNull(snapshotId, SNAPSHOT_ID);
            this.timeout = timeValue == null ? DEFAULT_TIMEOUT : timeValue;
            this.waitForCompletion = waitForCompletion;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.jobId = in.readString();
            this.snapshotId = in.readString();
            this.timeout = in.readTimeValue();
            this.waitForCompletion = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeString(snapshotId);
            out.writeTimeValue(timeout);
            out.writeBoolean(waitForCompletion);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getJobId() {
            return jobId;
        }

        public String getSnapshotId() {
            return snapshotId;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public boolean isWaitForCompletion() {
            return waitForCompletion;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(jobId, request.jobId)
                && Objects.equals(timeout, request.timeout)
                && Objects.equals(snapshotId, request.snapshotId)
                && waitForCompletion == request.waitForCompletion;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, snapshotId, timeout, waitForCompletion);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(SNAPSHOT_ID.getPreferredName(), snapshotId);
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            builder.field(WAIT_FOR_COMPLETION.getPreferredName(), waitForCompletion);
            builder.endObject();
            return builder;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField NODE = new ParseField("node");
        public static final ParseField COMPLETED = new ParseField("completed");

        private static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            a -> new UpgradeJobModelSnapshotAction.Response((boolean) a[0], (String) a[1])
        );
        static {
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), COMPLETED);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), NODE);
        }

        public static UpgradeJobModelSnapshotAction.Response parseRequest(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final boolean completed;
        private final String node;

        public Response(boolean completed, String node) {
            this.completed = completed;
            this.node = node;
        }

        public Response(StreamInput in) throws IOException {
            this.completed = in.readBoolean();
            this.node = in.readOptionalString();
        }

        public boolean isCompleted() {
            return completed;
        }

        public String getNode() {
            return node;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(completed);
            out.writeOptionalString(node);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(COMPLETED.getPreferredName(), completed);
            if (node != null) {
                builder.field(NODE.getPreferredName(), node);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return completed == response.completed && Objects.equals(node, response.node);
        }

        @Override
        public int hashCode() {
            return Objects.hash(completed, node);
        }
    }
}
