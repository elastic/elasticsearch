/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.calendars.ScheduledEvent;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Response to adding ScheduledEvent(s) to a Machine Learning calendar
 */
public class PostCalendarEventResponse implements ToXContentObject {

    private final List<ScheduledEvent> scheduledEvents;
    public static final ParseField EVENTS = new ParseField("events");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<PostCalendarEventResponse, Void> PARSER =
        new ConstructingObjectParser<>("post_calendar_event_response",
            true,
            a -> new PostCalendarEventResponse((List<ScheduledEvent>)a[0]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(),
            (p, c) -> ScheduledEvent.PARSER.apply(p, null), EVENTS);
    }

    public static PostCalendarEventResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Create a new PostCalendarEventResponse containing the scheduled Events
     *
     * @param scheduledEvents The list of {@link ScheduledEvent} objects
     */
    public PostCalendarEventResponse(List<ScheduledEvent> scheduledEvents) {
        this.scheduledEvents = scheduledEvents;
    }

    public List<ScheduledEvent> getScheduledEvents() {
        return scheduledEvents;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(EVENTS.getPreferredName(), scheduledEvents);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode(){
        return Objects.hash(scheduledEvents);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PostCalendarEventResponse other = (PostCalendarEventResponse) obj;
        return Objects.equals(scheduledEvents, other.scheduledEvents);
    }
}
