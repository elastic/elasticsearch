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
package org.elasticsearch.protocol.xpack.ml;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CloseJobRequest extends ActionRequest {

    public static final String ALL_JOBS = "_all";

    private List<String> jobIds = new ArrayList<>();
    private TimeValue timeout;
    private boolean force;
    private boolean allowNoJobs = true;

    public static CloseJobRequest closeAllJobsRequest(){
        return new CloseJobRequest(ALL_JOBS);
    }

    public CloseJobRequest(String jobId) {
        jobIds.add(Objects.requireNonNull(jobId, "[jobId] must not be null"));
    }

    CloseJobRequest() {
    }

    public void setJobIds(List<String> jobIds) {
        Objects.requireNonNull(jobIds, "jobIds must not be null");
        if (jobIds.stream().anyMatch(Objects::isNull)) {
            throw new NullPointerException("[jobId] must not be null");
        }
        this.jobIds = new ArrayList<>(jobIds);
    }

    public void addJobId(String jobId) {
        jobIds.add(Objects.requireNonNull(jobId, "[jobId] must not be null"));
    }

    public List<String> getJobIds() {
        return jobIds;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public boolean isAllowNoJobs() {
        return allowNoJobs;
    }

    public void setAllowNoJobs(boolean allowNoJobs) {
        this.allowNoJobs = allowNoJobs;
    }

    public String getCommaDelimitedJobIdString() {
        return Strings.collectionToCommaDelimitedString(jobIds);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobIds, timeout, allowNoJobs, force);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        CloseJobRequest that = (CloseJobRequest) other;
        return Objects.equals(jobIds, that.jobIds) &&
            Objects.equals(timeout, that.timeout) &&
            allowNoJobs == that.allowNoJobs &&
            force == that.force;
    }
}
