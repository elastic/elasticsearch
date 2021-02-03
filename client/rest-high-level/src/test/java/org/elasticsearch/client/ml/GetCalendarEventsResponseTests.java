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

public class GetCalendarEventsResponseTests extends AbstractXContentTestCase<GetCalendarEventsResponse> {

    @Override
    protected GetCalendarEventsResponse createTestInstance() {
        String calendarId = randomAlphaOfLength(10);
        List<ScheduledEvent> scheduledEvents = new ArrayList<>();
        int count = randomIntBetween(0, 3);
        for (int i=0; i<count; i++) {
            scheduledEvents.add(ScheduledEventTests.testInstance(calendarId, randomAlphaOfLength(10)));
        }
        return new GetCalendarEventsResponse(scheduledEvents, count);
    }

    @Override
    protected GetCalendarEventsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetCalendarEventsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
