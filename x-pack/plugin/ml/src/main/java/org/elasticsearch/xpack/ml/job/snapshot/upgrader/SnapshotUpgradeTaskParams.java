/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.snapshot.upgrader;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.MlTaskParams;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME;

public class SnapshotUpgradeTaskParams implements PersistentTaskParams, MlTaskParams {

    public static final ParseField SNAPSHOT_ID = new ParseField("snapshot_id");

    public static final ConstructingObjectParser<SnapshotUpgradeTaskParams, Void> PARSER = new ConstructingObjectParser<>(
        JOB_SNAPSHOT_UPGRADE_TASK_NAME,
        true,
        a -> new SnapshotUpgradeTaskParams((String) a[0], (String) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_ID);
    }

    public static final String NAME = JOB_SNAPSHOT_UPGRADE_TASK_NAME;

    private final String jobId;
    private final String snapshotId;

    public SnapshotUpgradeTaskParams(StreamInput in) throws IOException {
        this.jobId = in.readString();
        this.snapshotId = in.readString();
    }

    public SnapshotUpgradeTaskParams(String jobId, String snapshotId) {
        this.jobId = jobId;
        this.snapshotId = snapshotId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    @Override
    public String getWriteableName() {
        return JOB_SNAPSHOT_UPGRADE_TASK_NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_11_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeString(snapshotId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(SNAPSHOT_ID.getPreferredName(), snapshotId);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotUpgradeTaskParams params = (SnapshotUpgradeTaskParams) o;
        return Objects.equals(jobId, params.jobId) &&
            Objects.equals(snapshotId, params.snapshotId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, snapshotId);
    }

    @Override
    public String getMlId() {
        return jobId;
    }
}


