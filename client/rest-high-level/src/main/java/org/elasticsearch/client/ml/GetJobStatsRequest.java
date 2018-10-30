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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;


/**
 * Request object to get {@link org.elasticsearch.client.ml.job.stats.JobStats} by their respective jobIds
 *
 * {@code _all} explicitly gets all the jobs' statistics in the cluster
 * An empty request (no {@code jobId}s) implicitly gets all the jobs' statistics in the cluster
 */
public class GetJobStatsRequest extends ActionRequest implements ToXContentObject {

    public static final ParseField ALLOW_NO_JOBS = new ParseField("allow_no_jobs");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetJobStatsRequest, Void> PARSER = new ConstructingObjectParser<>(
        "get_jobs_stats_request", a -> new GetJobStatsRequest((List<String>) a[0]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            p -> Arrays.asList(Strings.commaDelimitedListToStringArray(p.text())),
            Job.ID, ObjectParser.ValueType.STRING_ARRAY);
        PARSER.declareBoolean(GetJobStatsRequest::setAllowNoJobs, ALLOW_NO_JOBS);
    }

    private static final String ALL_JOBS = "_all";

    private final List<String> jobIds;
    private Boolean allowNoJobs;

    /**
     * Explicitly gets all jobs statistics
     *
     * @return a {@link GetJobStatsRequest} for all existing jobs
     */
    public static GetJobStatsRequest getAllJobStatsRequest(){
        return new GetJobStatsRequest(ALL_JOBS);
    }

    GetJobStatsRequest(List<String> jobIds) {
        if (jobIds.stream().anyMatch(Objects::isNull)) {
            throw new NullPointerException("jobIds must not contain null values");
        }
        this.jobIds = new ArrayList<>(jobIds);
    }

    /**
     * Get the specified Job's statistics via their unique jobIds
     *
     * @param jobIds must be non-null and each jobId must be non-null
     */
    public GetJobStatsRequest(String... jobIds) {
        this(Arrays.asList(jobIds));
    }

    /**
     * All the jobIds for which to get statistics
     */
    public List<String> getJobIds() {
        return jobIds;
    }

    public Boolean getAllowNoJobs() {
        return this.allowNoJobs;
    }

    /**
     * Whether to ignore if a wildcard expression matches no jobs.
     *
     * This includes {@code _all} string or when no jobs have been specified
     *
     * @param allowNoJobs When {@code true} ignore if wildcard or {@code _all} matches no jobs. Defaults to {@code true}
     */
    public void setAllowNoJobs(boolean allowNoJobs) {
        this.allowNoJobs = allowNoJobs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobIds, allowNoJobs);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        GetJobStatsRequest that = (GetJobStatsRequest) other;
        return Objects.equals(jobIds, that.jobIds) &&
            Objects.equals(allowNoJobs, that.allowNoJobs);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), Strings.collectionToCommaDelimitedString(jobIds));
        if (allowNoJobs != null) {
            builder.field(ALLOW_NO_JOBS.getPreferredName(), allowNoJobs);
        }
        builder.endObject();
        return builder;
    }
}
