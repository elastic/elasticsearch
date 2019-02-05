/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.client.ml.calendars.Calendar;
import org.elasticsearch.client.ml.calendars.ScheduledEvent;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Request to add a ScheduledEvent to a Machine Learning calendar
 */
public class PostCalendarEventRequest extends ActionRequest implements ToXContentObject {

    private final String calendarId;
    private final List<ScheduledEvent> scheduledEvents;

    public static final String INCLUDE_CALENDAR_ID_KEY = "include_calendar_id";
    public static final ParseField EVENTS = new ParseField("events");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<PostCalendarEventRequest, Void> PARSER =
        new ConstructingObjectParser<>("post_calendar_event_request",
            a -> new PostCalendarEventRequest((String)a[0], (List<ScheduledEvent>)a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Calendar.ID);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(),
            (p, c) -> ScheduledEvent.PARSER.apply(p, null), EVENTS);
    }
    public static final MapParams EXCLUDE_CALENDAR_ID_PARAMS =
        new MapParams(Collections.singletonMap(INCLUDE_CALENDAR_ID_KEY, Boolean.toString(false)));

    /**
     * Create a new PostCalendarEventRequest with an existing non-null calendarId and a list of Scheduled events
     *
     * @param calendarId The ID of the calendar, must be non-null
     * @param scheduledEvents The non-null, non-empty, list of {@link ScheduledEvent} objects to add to the calendar
     */
    public PostCalendarEventRequest(String calendarId, List<ScheduledEvent> scheduledEvents) {
        this.calendarId = Objects.requireNonNull(calendarId, "[calendar_id] must not be null.");
        this.scheduledEvents = Objects.requireNonNull(scheduledEvents, "[events] must not be null.");
        if (scheduledEvents.isEmpty()) {
            throw new IllegalArgumentException("At least 1 event is required");
        }
    }

    public String getCalendarId() {
        return calendarId;
    }

    public List<ScheduledEvent> getScheduledEvents() {
        return scheduledEvents;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(INCLUDE_CALENDAR_ID_KEY, true)) {
            builder.field(Calendar.ID.getPreferredName(), calendarId);
        }
        builder.field(EVENTS.getPreferredName(), scheduledEvents);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(calendarId, scheduledEvents);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PostCalendarEventRequest other = (PostCalendarEventRequest) obj;
        return Objects.equals(calendarId, other.calendarId) && Objects.equals(scheduledEvents, other.scheduledEvents);
    }
}
