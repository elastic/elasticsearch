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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class CloseJobRequest extends ActionRequest implements ToXContentObject {

    public static final ParseField JOB_IDS = new ParseField("job_ids");
    public static final ParseField TIMEOUT = new ParseField("timeout");
    public static final ParseField FORCE = new ParseField("force");
    public static final ParseField ALLOW_NO_JOBS = new ParseField("allow_no_jobs");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<CloseJobRequest, Void> PARSER = new ConstructingObjectParser<>(
        "close_job_request",
        true, a -> new CloseJobRequest((List<String>) a[0]));

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), JOB_IDS);
        PARSER.declareString((obj, val) -> obj.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        PARSER.declareBoolean(CloseJobRequest::setForce, FORCE);
        PARSER.declareBoolean(CloseJobRequest::setAllowNoJobs, ALLOW_NO_JOBS);
    }

    private static final String ALL_JOBS = "_all";

    private final List<String> jobIds;
    private TimeValue timeout;
    private Boolean force;
    private Boolean allowNoJobs;

    /**
     * Explicitly close all jobs
     *
     * @return a {@link CloseJobRequest} for all existing jobs
     */
    public static CloseJobRequest closeAllJobsRequest(){
        return new CloseJobRequest(ALL_JOBS);
    }

    CloseJobRequest(List<String> jobIds) {
        if (jobIds.isEmpty()) {
            throw new InvalidParameterException("jobIds must not be empty");
        }
        if (jobIds.stream().anyMatch(Objects::isNull)) {
            throw new NullPointerException("jobIds must not contain null values");
        }
        this.jobIds = new ArrayList<>(jobIds);
    }

    /**
     * Close the specified Jobs via their unique jobIds
     *
     * @param jobIds must be non-null and non-empty and each jobId must be non-null
     */
    public CloseJobRequest(String... jobIds) {
        this(Arrays.asList(jobIds));
    }

    /**
     * All the jobIds to be closed
     */
    public List<String> getJobIds() {
        return jobIds;
    }

    /**
     * How long to wait for the close request to complete before timing out.
     *
     * Default: 30 minutes
     */
    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * {@link CloseJobRequest#getTimeout()}
     */
    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    /**
     * Should the closing be forced.
     *
     * Use to close a failed job, or to forcefully close a job which has not responded to its initial close request.
     */
    public Boolean isForce() {
        return force;
    }

    /**
     * {@link CloseJobRequest#isForce()}
     */
    public void setForce(boolean force) {
        this.force = force;
    }

    /**
     * Whether to ignore if a wildcard expression matches no jobs.
     *
     * This includes `_all` string or when no jobs have been specified
     */
    public Boolean isAllowNoJobs() {
        return allowNoJobs;
    }

    /**
     * {@link CloseJobRequest#isAllowNoJobs()}
     */
    public void setAllowNoJobs(boolean allowNoJobs) {
        this.allowNoJobs = allowNoJobs;
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
            Objects.equals(allowNoJobs, that.allowNoJobs) &&
            Objects.equals(force, that.force);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(JOB_IDS.getPreferredName(), jobIds);

        if (timeout != null) {
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
        }
        if (force != null) {
            builder.field(FORCE.getPreferredName(), force);
        }
        if (allowNoJobs != null) {
            builder.field(ALLOW_NO_JOBS.getPreferredName(), allowNoJobs);
        }

        builder.endObject();
        return builder;
    }
}
