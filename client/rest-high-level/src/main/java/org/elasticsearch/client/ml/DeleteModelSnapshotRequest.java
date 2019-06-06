/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.process.ModelSnapshot;


import java.util.Objects;

/**
 * Request to delete a Machine Learning Model Snapshot Job via its Job and Snapshot IDs
 */
public class DeleteModelSnapshotRequest extends ActionRequest {

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
    public ActionRequestValidationException validate() {
       return null;
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
