/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ServerSentEventParserTests extends ESTestCase {
    public void testRetryAndIdAreUnimplemented() {
        var payload = """
            id: 2

            retry: 2

            """.getBytes(StandardCharsets.UTF_8);

        var parser = new ServerSentEventParser();
        var events = parser.parse(payload);

        assertTrue(events.isEmpty());
    }

    public void testEmptyEventDefaultsToMessage() {
        var payload = """
            data: hello

            """.getBytes(StandardCharsets.UTF_8);

        var parser = new ServerSentEventParser();
        var events = parser.parse(payload);

        assertEvents(events, List.of(new ServerSentEvent("message", "hello")));
    }

    public void testEmptyData() {
        var payload = """
            data
            data

            """.getBytes(StandardCharsets.UTF_8);

        var parser = new ServerSentEventParser();
        var events = parser.parse(payload);

        assertEvents(events, List.of(new ServerSentEvent("message", "\n")));
    }

    public void testParseDataEventsWithAllEndOfLines() {
        var payload = """
            event: message\n\
            data: test\n\
            \n\
            event: message\r\
            data: test2\r\
            \r\
            event: message\r\n\
            data: test3\r\n\
            \r\n\
            """.getBytes(StandardCharsets.UTF_8);

        var parser = new ServerSentEventParser();
        var events = parser.parse(payload);

        assertEvents(
            events,
            List.of(
                new ServerSentEvent("message", "test"),
                new ServerSentEvent("message", "test2"),
                new ServerSentEvent("message", "test3")
            )
        );
    }

    public void testParseMultiLineDataEvents() {
        var payload = """
            event: message
            data: hello
            data: there

            """.getBytes(StandardCharsets.UTF_8);

        var parser = new ServerSentEventParser();
        var events = parser.parse(payload);

        assertEvents(events, List.of(new ServerSentEvent("message", "hello\nthere")));
    }

    private void assertEvents(Deque<ServerSentEvent> actualEvents, List<ServerSentEvent> expectedEvents) {
        assertThat(actualEvents.size(), equalTo(expectedEvents.size()));
        var expectedEvent = expectedEvents.iterator();
        actualEvents.forEach(event -> assertThat(event, equalTo(expectedEvent.next())));
    }

    // by default, Java's UTF-8 decode does not remove the byte order mark
    public void testByteOrderMarkIsRemoved() {
        // these are the bytes for "<byte-order mark>data: hello\n\n"
        var payload = new byte[] { -17, -69, -65, 100, 97, 116, 97, 58, 32, 104, 101, 108, 108, 111, 10, 10 };

        var parser = new ServerSentEventParser();
        var events = parser.parse(payload);

        assertEvents(events, List.of(new ServerSentEvent("message", "hello")));
    }

    public void testEmptyEventIsSetAsEmptyString() {
        var payload = """
            event:
            event:\s

            """.getBytes(StandardCharsets.UTF_8);

        var parser = new ServerSentEventParser();
        var events = parser.parse(payload);

        assertTrue(events.isEmpty());
    }

    public void testCommentsAreIgnored() {
        var parser = new ServerSentEventParser();

        var events = parser.parse("""
            :some cool comment
            :event: message

            """.getBytes(StandardCharsets.UTF_8));

        assertTrue(events.isEmpty());
    }

    public void testCarryOverBytes() {
        var parser = new ServerSentEventParser();

        var events = parser.parse("""
            event: message
            data""".getBytes(StandardCharsets.UTF_8)); // no newline after 'data' so the parser won't split the message up

        assertTrue(events.isEmpty());

        events = parser.parse("""
            :test

            """.getBytes(StandardCharsets.UTF_8));

        assertEvents(events, List.of(new ServerSentEvent("test")));
    }
}
