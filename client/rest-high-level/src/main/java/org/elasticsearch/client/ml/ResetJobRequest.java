/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;

import java.util.Objects;

/**
 * Request to delete a Machine Learning Job via its ID
 */
public class ResetJobRequest extends ActionRequest {

    private String jobId;
    private Boolean waitForCompletion;

    public ResetJobRequest(String jobId) {
        this.jobId = Objects.requireNonNull(jobId, "[job_id] must not be null");
    }

    public String getJobId() {
        return jobId;
    }

    /**
     * The jobId which to reset
     * @param jobId unique jobId to reset, must not be null
     */
    public void setJobId(String jobId) {
        this.jobId = Objects.requireNonNull(jobId, "[job_id] must not be null");
    }

    public Boolean getWaitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Set whether this request should wait until the operation has completed before returning
     * @param waitForCompletion When {@code true} the call will wait for the job reset to complete.
     *                          Otherwise, the reset will be executed asynchronously and the response
     *                          will contain the task id.
     */
    public void setWaitForCompletion(Boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, waitForCompletion);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        ResetJobRequest other = (ResetJobRequest) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(waitForCompletion, other.waitForCompletion);
    }

}
