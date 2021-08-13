/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.Optional;

/**
 * Request to delete a data frame analytics config
 */
public class DeleteDataFrameAnalyticsRequest implements Validatable {

    private final String id;
    private Boolean force;
    private TimeValue timeout;

    public DeleteDataFrameAnalyticsRequest(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Boolean getForce() {
        return force;
    }

    /**
     * Used to forcefully delete an job that is not stopped.
     * This method is quicker than stopping and deleting the job.
     *
     * @param force When {@code true} forcefully delete a non stopped job. Defaults to {@code false}
     */
    public void setForce(Boolean force) {
        this.force = force;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * Sets the time to wait until the job is deleted.
     *
     * @param timeout The time to wait until the job is deleted.
     */
    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (id == null) {
            return Optional.of(ValidationException.withError("data frame analytics id must not be null"));
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeleteDataFrameAnalyticsRequest other = (DeleteDataFrameAnalyticsRequest) o;
        return Objects.equals(id, other.id)
            && Objects.equals(force, other.force)
            && Objects.equals(timeout, other.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, force, timeout);
    }
}
