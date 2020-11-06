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

import org.elasticsearch.client.ml.calendars.ScheduledEvent;
import org.elasticsearch.common.ParseField;
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
