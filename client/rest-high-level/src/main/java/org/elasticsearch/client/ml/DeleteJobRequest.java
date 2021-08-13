/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;

import java.util.Objects;

/**
 * Request to delete a Machine Learning Job via its ID
 */
public class DeleteJobRequest implements Validatable {

    private String jobId;
    private Boolean force;
    private Boolean waitForCompletion;

    public DeleteJobRequest(String jobId) {
        this.jobId = Objects.requireNonNull(jobId, "[job_id] must not be null");
    }

    public String getJobId() {
        return jobId;
    }

    /**
     * The jobId which to delete
     * @param jobId unique jobId to delete, must not be null
     */
    public void setJobId(String jobId) {
        this.jobId = Objects.requireNonNull(jobId, "[job_id] must not be null");
    }

    public Boolean getForce() {
        return force;
    }

    /**
     * Used to forcefully delete an opened job.
     * This method is quicker than closing and deleting the job.
     *
     * @param force When {@code true} forcefully delete an opened job. Defaults to {@code false}
     */
    public void setForce(Boolean force) {
        this.force = force;
    }

    public Boolean getWaitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Set whether this request should wait until the operation has completed before returning
     * @param waitForCompletion When {@code true} the call will wait for the job deletion to complete.
     *                          Otherwise, the deletion will be executed asynchronously and the response
     *                          will contain the task id.
     */
    public void setWaitForCompletion(Boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, force);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        DeleteJobRequest other = (DeleteJobRequest) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(force, other.force);
    }

}
