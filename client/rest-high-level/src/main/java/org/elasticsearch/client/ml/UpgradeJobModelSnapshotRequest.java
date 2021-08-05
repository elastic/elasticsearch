/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class UpgradeJobModelSnapshotRequest implements Validatable, ToXContentObject {

    public static final ParseField SNAPSHOT_ID = new ParseField("snapshot_id");
    public static final ParseField TIMEOUT = new ParseField("timeout");
    public static final ParseField WAIT_FOR_COMPLETION = new ParseField("wait_for_completion");

    private static final ConstructingObjectParser<UpgradeJobModelSnapshotRequest, Void> PARSER = new ConstructingObjectParser<>(
        "upgrade_job_snapshot_request",
        true,
        a -> new UpgradeJobModelSnapshotRequest((String) a[0], (String) a[1], (String) a[2], (Boolean) a[3]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_ID);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TIMEOUT);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), WAIT_FOR_COMPLETION);
    }

    private final String jobId;
    private final String snapshotId;
    private final TimeValue timeout;
    private final Boolean waitForCompletion;

    UpgradeJobModelSnapshotRequest(String jobId, String snapshotId, String timeout, Boolean waitForCompletion) {
        this(jobId,
            snapshotId,
            timeout == null ? null : TimeValue.parseTimeValue(timeout, TIMEOUT.getPreferredName()),
            waitForCompletion);
    }

    public UpgradeJobModelSnapshotRequest(String jobId, String snapshotId, TimeValue timeValue, Boolean waitForCompletion) {
        this.jobId = Objects.requireNonNull(jobId, Job.ID.getPreferredName());
        this.snapshotId = Objects.requireNonNull(snapshotId, SNAPSHOT_ID.getPreferredName());
        this.timeout = timeValue;
        this.waitForCompletion = waitForCompletion;
    }

    public static UpgradeJobModelSnapshotRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
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

    public Boolean getWaitForCompletion() {
        return waitForCompletion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UpgradeJobModelSnapshotRequest request = (UpgradeJobModelSnapshotRequest) o;
        return Objects.equals(jobId, request.jobId) &&
            Objects.equals(timeout, request.timeout) &&
            Objects.equals(waitForCompletion, request.waitForCompletion) &&
            Objects.equals(snapshotId, request.snapshotId);
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
        if (timeout != null) {
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
        }
        if (waitForCompletion != null) {
            builder.field(WAIT_FOR_COMPLETION.getPreferredName(), waitForCompletion);
        }
        builder.endObject();
        return builder;
    }

}
