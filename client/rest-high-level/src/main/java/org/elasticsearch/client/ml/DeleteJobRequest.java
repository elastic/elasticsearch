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

import java.util.Objects;

/**
 * Request to delete a Machine Learning Job via its ID
 */
public class DeleteJobRequest extends ActionRequest {

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
    public ActionRequestValidationException validate() {
       return null;
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
