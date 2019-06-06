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

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Request class for adding Machine Learning Jobs to an existing calendar
 */
public class PutCalendarJobRequest extends ActionRequest {

    private final List<String> jobIds;
    private final String calendarId;

    /**
     * Create a new request referencing an existing Calendar and which JobIds to add
     * to it.
     *
     * @param calendarId The non-null ID of the calendar
     * @param jobIds JobIds to add to the calendar, cannot be empty, or contain null values.
     *               It can be a list of jobs or groups.
     */
    public PutCalendarJobRequest(String calendarId, String... jobIds) {
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
    public ActionRequestValidationException validate() {
        return null;
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

        PutCalendarJobRequest that = (PutCalendarJobRequest) other;
        return Objects.equals(jobIds, that.jobIds) &&
            Objects.equals(calendarId, that.calendarId);
    }
}
