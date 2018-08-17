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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Request object to get {@link org.elasticsearch.protocol.xpack.ml.job.config.Job} objects with the matching `jobId`s or
 * `groupName`s.
 *
 * `_all` (see {@link GetJobRequest#ALL_JOBS}) `jobId` explicitly gets all the jobs in the cluster
 * An empty request (no `jobId`s) implicitly gets all the jobs in the cluster
 *
 */
public class GetJobRequest extends ActionRequest {

    public static final String ALL_JOBS = "_all";
    private List<String> jobIds = new ArrayList<>();
    private boolean allowNoJobs = true;

    /**
     * Helper method to create a query that will get ALL jobs
     * @return new {@link GetJobRequest} object searching for the jobId "_all"
     */
    public static GetJobRequest getAllJobsRequest() {
        return new GetJobRequest(ALL_JOBS);
    }

    public GetJobRequest(String jobId) {
        jobIds.add(Objects.requireNonNull(jobId, "[jobId] must not be null"));
    }

    public GetJobRequest() {
    }

    /**
     * Is a comma delimited representation of the list of `jobId`/`groupName` to get.
     *
     * @return String of jobIds/groupNames. example: "job1,other-jobs*"
     */
    public String getCommaDelimitedJobIdsString() {
        return Strings.collectionToCommaDelimitedString(jobIds);
    }

    /**
     * Adds a new non-null `jobId` or `groupName` to be included in the request.
     *
     * Can include wildcards, and can explicitly ask for all jobs by requesting with
     * the reserved {@link GetJobRequest#ALL_JOBS} name.
     *
     * @param jobId non-null jobId or groupName, accepts wildcards
     */
    public void addJobId(String jobId) {
        jobIds.add(Objects.requireNonNull(jobId, "[jobId] must not be null"));
    }

    /**
     * Whether to ignore if a wildcard expression matches no jobs.
     * This includes {@link GetJobRequest#ALL_JOBS} string or when no jobs have been specified.
     *
     * If this is `false`, then an error is returned when a wildcard (or `_all`) does not match any jobs
     *
     * Default value: `true`
     *
     * @param allowNoJobs Ignore finding no jobs or not
     */
    public void setAllowNoJobs(boolean allowNoJobs) {
        this.allowNoJobs = allowNoJobs;
    }

    public boolean isAllowNoJobs() {
        return allowNoJobs;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobIds, allowNoJobs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        GetJobRequest other = (GetJobRequest) obj;
        return Objects.equals(jobIds, other.jobIds) && allowNoJobs == other.allowNoJobs;
    }

}
