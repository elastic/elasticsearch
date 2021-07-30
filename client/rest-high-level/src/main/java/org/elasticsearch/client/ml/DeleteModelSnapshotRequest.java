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

import java.util.Objects;

/**
 * Request to delete a Machine Learning Model Snapshot Job via its Job and Snapshot IDs
 */
public class DeleteModelSnapshotRequest implements Validatable {

    private final String jobId;
    private final String snapshotId;

    public DeleteModelSnapshotRequest(String jobId, String snapshotId) {
        this.jobId = Objects.requireNonNull(jobId, "[" + Job.ID + "] must not be null");
        this.snapshotId = Objects.requireNonNull(snapshotId, "[" + ModelSnapshot.SNAPSHOT_ID + "] must not be null");
    }

    public String getJobId() {
        return jobId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, snapshotId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        DeleteModelSnapshotRequest other = (DeleteModelSnapshotRequest) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(snapshotId, other.snapshotId);
    }

}
