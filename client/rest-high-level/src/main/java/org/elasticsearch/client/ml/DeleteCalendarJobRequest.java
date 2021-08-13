/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Request class for removing Machine Learning Jobs from an existing calendar
 */
public class DeleteCalendarJobRequest implements Validatable {

    private final List<String> jobIds;
    private final String calendarId;

    /**
     * Create a new request referencing an existing Calendar and which JobIds to remove
     * from it.
     *
     * @param calendarId The non-null ID of the calendar
     * @param jobIds JobIds to remove from the calendar, cannot be empty, or contain null values.
     *               It can be a list of jobs or groups.
     */
    public DeleteCalendarJobRequest(String calendarId, String... jobIds) {
        this.calendarId = Objects.requireNonNull(calendarId, "[calendar_id] must not be null.");
        if (jobIds.length == 0) {
            throw new InvalidParameterException("jobIds must not be empty.");
        }
        if (Arrays.stream(jobIds).anyMatch(Objects::isNull)) {
            throw new NullPointerException("jobIds must not contain null values.");
        }
        this.jobIds = Arrays.asList(jobIds);
    }

    public List<String> getJobIds() {
        return jobIds;
    }

    public String getCalendarId() {
        return calendarId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobIds, calendarId);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DeleteCalendarJobRequest that = (DeleteCalendarJobRequest) other;
        return Objects.equals(jobIds, that.jobIds) &&
            Objects.equals(calendarId, that.calendarId);
    }
}
