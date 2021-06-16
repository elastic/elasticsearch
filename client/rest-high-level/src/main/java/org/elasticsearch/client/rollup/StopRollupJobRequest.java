/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;

public class StopRollupJobRequest implements Validatable {

    private final String jobId;
    private TimeValue timeout;
    private Boolean waitForCompletion;

    public StopRollupJobRequest(final String jobId) {
        this.jobId = Objects.requireNonNull(jobId, "id parameter must not be null");
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final StopRollupJobRequest that = (StopRollupJobRequest) o;
        return Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId);
    }

    /**
     * Sets the requests optional "timeout" parameter.
     */
    public void timeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    public TimeValue timeout() {
        return this.timeout;
    }

    /**
     * Sets the requests optional "wait_for_completion".
     */
    public void waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    public Boolean waitForCompletion() {
        return this.waitForCompletion;
    }
}
