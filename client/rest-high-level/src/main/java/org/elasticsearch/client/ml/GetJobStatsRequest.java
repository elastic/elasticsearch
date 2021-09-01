/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.xcontent.ParseField;
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
public class GetJobStatsRequest implements Validatable, ToXContentObject {

    public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetJobStatsRequest, Void> PARSER = new ConstructingObjectParser<>(
        "get_jobs_stats_request", a -> new GetJobStatsRequest((List<String>) a[0]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            p -> Arrays.asList(Strings.commaDelimitedListToStringArray(p.text())),
            Job.ID, ObjectParser.ValueType.STRING_ARRAY);
        PARSER.declareBoolean(GetJobStatsRequest::setAllowNoMatch, ALLOW_NO_MATCH);
    }

    private static final String ALL_JOBS = "_all";

    private final List<String> jobIds;
    private Boolean allowNoMatch;

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
        return Objects.hash(jobIds, allowNoMatch);
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
            Objects.equals(allowNoMatch, that.allowNoMatch);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), Strings.collectionToCommaDelimitedString(jobIds));
        if (allowNoMatch != null) {
            builder.field(ALLOW_NO_MATCH.getPreferredName(), allowNoMatch);
        }
        builder.endObject();
        return builder;
    }
}
