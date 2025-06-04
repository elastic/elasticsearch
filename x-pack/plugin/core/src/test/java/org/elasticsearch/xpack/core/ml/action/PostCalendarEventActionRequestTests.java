/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEventTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PostCalendarEventActionRequestTests extends AbstractWireSerializingTestCase<PostCalendarEventsAction.Request> {

    @Override
    protected PostCalendarEventsAction.Request createTestInstance() {
        String id = randomAlphaOfLengthBetween(1, 20);
        return createTestInstance(id);
    }

    @Override
    protected PostCalendarEventsAction.Request mutateInstance(PostCalendarEventsAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<PostCalendarEventsAction.Request> instanceReader() {
        return PostCalendarEventsAction.Request::new;
    }

    private PostCalendarEventsAction.Request createTestInstance(String calendarId) {
        int numEvents = randomIntBetween(1, 10);
        List<ScheduledEvent> events = new ArrayList<>();
        for (int i = 0; i < numEvents; i++) {
            events.add(ScheduledEventTests.createScheduledEvent(calendarId));
        }

        PostCalendarEventsAction.Request request = new PostCalendarEventsAction.Request(calendarId, events);
        return request;
    }

    public void testParseRequest() throws IOException {
        PostCalendarEventsAction.Request sourceRequest = createTestInstance();

        StringBuilder requestString = new StringBuilder();
        requestString.append("{\"events\": [");
        for (ScheduledEvent event : sourceRequest.getScheduledEvents()) {
            requestString.append(Strings.toString(event)).append(',');
        }
        requestString.replace(requestString.length() - 1, requestString.length(), "]");
        requestString.append('}');

        XContentParser parser = createParser(XContentType.JSON.xContent(), requestString.toString());
        PostCalendarEventsAction.Request parsedRequest = PostCalendarEventsAction.Request.parseRequest(
            sourceRequest.getCalendarId(),
            parser
        );

        assertEquals(sourceRequest, parsedRequest);
    }

    public void testParseRequest_throwsIfCalendarIdsAreDifferent() throws IOException {
        PostCalendarEventsAction.Request sourceRequest = createTestInstance("foo");

        StringBuilder requestString = new StringBuilder();
        requestString.append("{\"events\": [");
        for (ScheduledEvent event : sourceRequest.getScheduledEvents()) {
            requestString.append(Strings.toString(event)).append(',');
        }
        requestString.replace(requestString.length() - 1, requestString.length(), "]");
        requestString.append('}');

        XContentParser parser = createParser(XContentType.JSON.xContent(), requestString.toString());
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> PostCalendarEventsAction.Request.parseRequest("bar", parser)
        );
        assertEquals(
            "Inconsistent calendar_id; 'foo' specified in the body differs from 'bar' specified as a URL argument",
            e.getMessage()
        );
    }
}
