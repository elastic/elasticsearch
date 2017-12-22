/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.ml.calendars.SpecialEvent;
import org.elasticsearch.xpack.ml.calendars.SpecialEventTests;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class PostCalendarEventActionRequestTests extends AbstractStreamableTestCase<PostCalendarEventsAction.Request> {

    @Override
    protected PostCalendarEventsAction.Request createTestInstance() {
        String id = randomAlphaOfLengthBetween(1, 20);
        return createTestInstance(id);
    }

    private PostCalendarEventsAction.Request createTestInstance(String calendarId) {
        int numEvents = randomIntBetween(1, 10);
        List<SpecialEvent> events = new ArrayList<>();
        for (int i=0; i<numEvents; i++) {
            events.add(SpecialEventTests.createSpecialEvent(calendarId));
        }

        PostCalendarEventsAction.Request request = new PostCalendarEventsAction.Request(calendarId, events);
        return request;
    }

    @Override
    protected PostCalendarEventsAction.Request createBlankInstance() {
        return new PostCalendarEventsAction.Request();
    }


    public void testParseRequest() throws IOException {
        PostCalendarEventsAction.Request sourceRequest = createTestInstance();

        StringBuilder requestString = new StringBuilder();
        for (SpecialEvent event: sourceRequest.getSpecialEvents()) {
            requestString.append(Strings.toString(event)).append("\r\n");
        }

        BytesArray data = new BytesArray(requestString.toString().getBytes(StandardCharsets.UTF_8), 0, requestString.length());
        PostCalendarEventsAction.Request parsedRequest = PostCalendarEventsAction.Request.parseRequest(
                sourceRequest.getCalendarId(), data, XContentType.JSON);

        assertEquals(sourceRequest, parsedRequest);
    }

    public void testParseRequest_throwsIfCalendarIdsAreDifferent() throws IOException {
        PostCalendarEventsAction.Request sourceRequest = createTestInstance("foo");
        PostCalendarEventsAction.Request request = new PostCalendarEventsAction.Request("bar", sourceRequest.getSpecialEvents());


        StringBuilder requestString = new StringBuilder();
        for (SpecialEvent event: sourceRequest.getSpecialEvents()) {
            requestString.append(Strings.toString(event)).append("\r\n");
        }

        BytesArray data = new BytesArray(requestString.toString().getBytes(StandardCharsets.UTF_8), 0, requestString.length());
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> PostCalendarEventsAction.Request.parseRequest(request.getCalendarId(), data, XContentType.JSON));
        assertEquals("Inconsistent calendar_id; 'foo' specified in the body differs from 'bar' specified as a URL argument",
                e.getMessage());
    }
}
