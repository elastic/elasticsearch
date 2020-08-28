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
