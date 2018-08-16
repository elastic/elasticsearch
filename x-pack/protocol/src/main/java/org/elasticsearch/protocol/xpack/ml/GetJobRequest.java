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

public class GetJobRequest extends ActionRequest {

    public final static String ALL_JOBS = "_all";
    private List<String> jobIds = new ArrayList<>();
    private boolean allowNoJobs = true;

    public static GetJobRequest getAllJobsRequest() {
        return new GetJobRequest(ALL_JOBS);
    }

    public GetJobRequest(String jobId) {
        jobIds.add(Objects.requireNonNull(jobId, "[jobId] must not be null"));
    }

    public GetJobRequest() {
    }

    public String getCommaDelimitedJobIdsString() {
        return jobIds.isEmpty() ? null : Strings.collectionToCommaDelimitedString(jobIds);
    }

    public void addJobId(String jobId) {
        jobIds.add(Objects.requireNonNull(jobId, "[jobId] must not be null"));
    }

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
