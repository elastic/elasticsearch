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

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Request to close Machine Learning Jobs
 */
public class CloseJobRequest implements ToXContentObject, Validatable {

    public static final ParseField JOB_ID = new ParseField("job_id");
    public static final ParseField TIMEOUT = new ParseField("timeout");
    public static final ParseField FORCE = new ParseField("force");
    public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<CloseJobRequest, Void> PARSER = new ConstructingObjectParser<>(
        "close_job_request",
        true, a -> new CloseJobRequest((List<String>) a[0]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            p -> Arrays.asList(Strings.commaDelimitedListToStringArray(p.text())),
            JOB_ID, ObjectParser.ValueType.STRING_ARRAY);
        PARSER.declareString((obj, val) -> obj.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        PARSER.declareBoolean(CloseJobRequest::setForce, FORCE);
        PARSER.declareBoolean(CloseJobRequest::setAllowNoMatch, ALLOW_NO_MATCH);
    }

    private static final String ALL_JOBS = "_all";

    private final List<String> jobIds;
    private TimeValue timeout;
    private Boolean force;
    private Boolean allowNoMatch;

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

    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * How long to wait for the close request to complete before timing out.
     *
     * @param timeout Default value: 30 minutes
     */
    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    public Boolean getForce() {
        return force;
    }

    /**
     * Should the closing be forced.
     *
     * Use to close a failed job, or to forcefully close a job which has not responded to its initial close request.
     *
     * @param force When {@code true} forcefully close the job. Defaults to {@code false}
     */
    public void setForce(boolean force) {
        this.force = force;
    }

    public Boolean getAllowNoMatch() {
        return this.allowNoMatch;
    }

    /**
     * Whether to ignore if a wildcard expression matches no jobs.
     *
     * This includes {@code _all} string or when no jobs have been specified
     *
     * @param allowNoMatch When {@code true} ignore if wildcard or {@code _all} matches no jobs. Defaults to {@code true}
     */
    public void setAllowNoMatch(boolean allowNoMatch) {
        this.allowNoMatch = allowNoMatch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobIds, timeout, force, allowNoMatch);
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
            Objects.equals(force, that.force) &&
            Objects.equals(allowNoMatch, that.allowNoMatch);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(JOB_ID.getPreferredName(), Strings.collectionToCommaDelimitedString(jobIds));
        if (timeout != null) {
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
        }
        if (force != null) {
            builder.field(FORCE.getPreferredName(), force);
        }
        if (allowNoMatch != null) {
            builder.field(ALLOW_NO_MATCH.getPreferredName(), allowNoMatch);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
