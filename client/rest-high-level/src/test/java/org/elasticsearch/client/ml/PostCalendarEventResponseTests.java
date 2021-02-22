/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.calendars.ScheduledEvent;
import org.elasticsearch.client.ml.calendars.ScheduledEventTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PostCalendarEventResponseTests extends AbstractXContentTestCase<PostCalendarEventResponse> {
    @Override
    protected PostCalendarEventResponse createTestInstance() {
        int numberOfEvents = randomIntBetween(1, 10);
        List<ScheduledEvent> events = new ArrayList<>(numberOfEvents);
        for (int i = 0; i < numberOfEvents; i++) {
            events.add(ScheduledEventTests.testInstance());
        }
        return new PostCalendarEventResponse(events);
    }

    @Override
    protected PostCalendarEventResponse doParseInstance(XContentParser parser) throws IOException {
        return PostCalendarEventResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
