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
import org.elasticsearch.client.ml.job.process.ModelSnapshot;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A request to revert to a specific model snapshot for a given job
 */
public class RevertModelSnapshotRequest implements Validatable, ToXContentObject {


    public static final ParseField DELETE_INTERVENING = new ParseField("delete_intervening_results");

    public static final ConstructingObjectParser<RevertModelSnapshotRequest, Void> PARSER = new ConstructingObjectParser<>(
        "revert_model_snapshots_request", a -> new RevertModelSnapshotRequest((String) a[0], (String) a[1]));


    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ModelSnapshot.SNAPSHOT_ID);
        PARSER.declareBoolean(RevertModelSnapshotRequest::setDeleteInterveningResults, DELETE_INTERVENING);
    }

    private final String jobId;
    private final String snapshotId;
    private Boolean deleteInterveningResults;

    /**
     * Constructs a request to revert to a given model snapshot
     * @param jobId id of the job for which to revert the model snapshot
     * @param snapshotId id of the snapshot to which to revert
     */
    public RevertModelSnapshotRequest(String jobId, String snapshotId) {
        this.jobId = Objects.requireNonNull(jobId, "[" + Job.ID + "] must not be null");
        this.snapshotId = Objects.requireNonNull(snapshotId, "[" + ModelSnapshot.SNAPSHOT_ID + "] must not be null");
    }

    public String getJobId() {
        return jobId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public Boolean getDeleteInterveningResults() {
        return deleteInterveningResults;
    }

    /**
     * Sets the request flag that indicates whether or not intervening results should be deleted.
     * @param deleteInterveningResults Flag that indicates whether or not intervening results should be deleted.
     */
    public void setDeleteInterveningResults(Boolean deleteInterveningResults) {
        this.deleteInterveningResults = deleteInterveningResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(ModelSnapshot.SNAPSHOT_ID.getPreferredName(), snapshotId);
        if (deleteInterveningResults != null) {
            builder.field(DELETE_INTERVENING.getPreferredName(), deleteInterveningResults);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RevertModelSnapshotRequest request = (RevertModelSnapshotRequest) obj;
        return Objects.equals(jobId, request.jobId)
            && Objects.equals(snapshotId, request.snapshotId)
            && Objects.equals(deleteInterveningResults, request.deleteInterveningResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, snapshotId, deleteInterveningResults);
    }
}
