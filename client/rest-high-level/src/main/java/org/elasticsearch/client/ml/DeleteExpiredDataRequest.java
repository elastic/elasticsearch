/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to delete expired model snapshots and forecasts
 */
public class DeleteExpiredDataRequest implements Validatable, ToXContentObject {

    static final String REQUESTS_PER_SECOND = "requests_per_second";
    static final String TIMEOUT = "timeout";
    static final String JOB_ID = "job_id";

    private final String jobId;
    private final Float requestsPerSecond;
    private final TimeValue timeout;

   /**
     * Create a new request to delete expired data
     */
    public DeleteExpiredDataRequest() {
        this(null, null, null);
    }

    public DeleteExpiredDataRequest(String jobId, Float requestsPerSecond, TimeValue timeout) {
        this.jobId = jobId;
        this.requestsPerSecond = requestsPerSecond;
        this.timeout = timeout;
    }

    /**
     * The requests allowed per second in the underlying Delete by Query requests executed.
     *
     * `-1.0f` indicates that the standard nightly cleanup behavior should be ran.
     *         Throttling scales according to the number of data nodes.
     * `null` is default and means no throttling will occur.
     */
    public Float getRequestsPerSecond() {
        return requestsPerSecond;
    }

    /**
     * Indicates how long the deletion request will run until it timesout.
     *
     * Default value is 8 hours.
     */
    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * The optional job id
     *
     * The default is `null` meaning all jobs.
     * @return The job id or null
     */
    public String getJobId() {
        return jobId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteExpiredDataRequest that = (DeleteExpiredDataRequest) o;
        return Objects.equals(requestsPerSecond, that.requestsPerSecond) &&
            Objects.equals(timeout, that.timeout) &&
            Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestsPerSecond, timeout, jobId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (jobId != null) {
            builder.field(JOB_ID, jobId);
        }
        if (requestsPerSecond != null) {
            builder.field(REQUESTS_PER_SECOND, requestsPerSecond);
        }
        if (timeout != null) {
            builder.field(TIMEOUT, timeout.getStringRep());
        }
        builder.endObject();
        return builder;
    }
}
