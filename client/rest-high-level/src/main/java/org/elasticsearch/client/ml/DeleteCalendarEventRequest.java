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
 * Request class for removing an event from an existing calendar
 */
public class DeleteCalendarEventRequest implements Validatable {

    private final String eventId;
    private final String calendarId;

    /**
     * Create a new request referencing an existing Calendar and which event to remove
     * from it.
     *
     * @param calendarId The non-null ID of the calendar
     * @param eventId Scheduled Event to remove from the calendar, Cannot be null.
     */
    public DeleteCalendarEventRequest(String calendarId, String eventId) {
        this.calendarId = Objects.requireNonNull(calendarId, "[calendar_id] must not be null.");
        this.eventId = Objects.requireNonNull(eventId, "[event_id] must not be null.");
    }

    public String getEventId() {
        return eventId;
    }

    public String getCalendarId() {
        return calendarId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, calendarId);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DeleteCalendarEventRequest that = (DeleteCalendarEventRequest) other;
        return Objects.equals(eventId, that.eventId) &&
            Objects.equals(calendarId, that.calendarId);
    }
}
