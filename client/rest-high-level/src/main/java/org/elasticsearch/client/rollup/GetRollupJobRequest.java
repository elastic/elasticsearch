/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.rollup;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;

import java.util.Objects;
import java.util.Optional;

/**
 * Request to fetch rollup jobs.
 */
public class GetRollupJobRequest implements Validatable {
    private final String jobId;

    /**
     * Create a requets .
     * @param jobId id of the job to return or {@code _all} to return all jobs
     */
    public GetRollupJobRequest(final String jobId) {
        Objects.requireNonNull(jobId, "jobId is required");
        if ("_all".equals(jobId)) {
            throw new IllegalArgumentException("use the default ctor to ask for all jobs");
        }
        this.jobId = jobId;
    }

    /**
     * Create a request to load all rollup jobs.
     */
    public GetRollupJobRequest() {
        this.jobId = "_all";
    }

    /**
     * ID of the job to return.
     */
    public String getJobId() {
        return jobId;
    }

    @Override
    public Optional<ValidationException> validate() {
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GetRollupJobRequest that = (GetRollupJobRequest) o;
        return jobId.equals(that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId);
    }
}
