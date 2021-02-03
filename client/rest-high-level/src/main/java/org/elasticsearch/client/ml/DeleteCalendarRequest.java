/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;

import java.util.Objects;

/**
 * Request to delete a Machine Learning Calendar
 */
public class DeleteCalendarRequest implements Validatable {

    private final String calendarId;

    /**
     * The constructor requires a single calendar id.
     * @param calendarId The calendar to delete. Must be {@code non-null}
     */
    public DeleteCalendarRequest(String calendarId) {
        this.calendarId = Objects.requireNonNull(calendarId, "[calendar_id] must not be null");
    }

    public String getCalendarId() {
        return calendarId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(calendarId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DeleteCalendarRequest other = (DeleteCalendarRequest) obj;
        return Objects.equals(calendarId, other.calendarId);
    }
}
