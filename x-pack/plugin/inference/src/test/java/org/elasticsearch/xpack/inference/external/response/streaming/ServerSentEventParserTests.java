/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class ServerSentEventParserTests extends ESTestCase {
    public void testParseEvents() {
        var payload = (Arrays.stream(ServerSentEventField.values())
            .map(ServerSentEventField::name)
            .map(name -> name.toLowerCase(Locale.ROOT))
            .collect(Collectors.joining("\n"))
            + "\n").getBytes(StandardCharsets.UTF_8);

        var parser = new ServerSentEventParser();
        var events = parser.parse(payload);

        assertThat(events.size(), equalTo(ServerSentEventField.values().length));
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
                new ServerSentEvent(ServerSentEventField.EVENT, "message"),
                new ServerSentEvent(ServerSentEventField.DATA, "test"),
                new ServerSentEvent(ServerSentEventField.EVENT, "message"),
                new ServerSentEvent(ServerSentEventField.DATA, "test2"),
                new ServerSentEvent(ServerSentEventField.EVENT, "message"),
                new ServerSentEvent(ServerSentEventField.DATA, "test3")
            )
        );
    }

    private void assertEvents(Deque<ServerSentEvent> actualEvents, List<ServerSentEvent> expectedEvents) {
        assertThat(actualEvents.size(), equalTo(expectedEvents.size()));
        var expectedEvent = expectedEvents.iterator();
        actualEvents.forEach(event -> assertThat(event, equalTo(expectedEvent.next())));
    }

    // by default, Java's UTF-8 decode does not remove the byte order mark
    public void testByteOrderMarkIsRemoved() {
        // these are the bytes for "<byte-order mark>event: message\n\n"
        var payload = new byte[] { -17, -69, -65, 101, 118, 101, 110, 116, 58, 32, 109, 101, 115, 115, 97, 103, 101, 10, 10 };

        var parser = new ServerSentEventParser();
        var events = parser.parse(payload);

        assertEvents(events, List.of(new ServerSentEvent(ServerSentEventField.EVENT, "message")));
    }

    public void testEmptyEventIsSetAsEmptyString() {
        var payload = """
            event:
            event:\s

            """.getBytes(StandardCharsets.UTF_8);

        var parser = new ServerSentEventParser();
        var events = parser.parse(payload);

        assertEvents(
            events,
            List.of(new ServerSentEvent(ServerSentEventField.EVENT, ""), new ServerSentEvent(ServerSentEventField.EVENT, ""))
        );
    }

    public void testCommentsAreIgnored() {
        var parser = new ServerSentEventParser();

        var events = parser.parse("""
            :some cool comment
            :event: message

            """.getBytes(StandardCharsets.UTF_8));

        assertThat(events.isEmpty(), equalTo(true));
    }

    public void testCarryOverBytes() {
        var parser = new ServerSentEventParser();

        var events = parser.parse("""
            event: message
            data""".getBytes(StandardCharsets.UTF_8)); // no newline after 'data' so the parser won't split the message up

        assertEvents(events, List.of(new ServerSentEvent(ServerSentEventField.EVENT, "message")));

        events = parser.parse("""
            :test

            """.getBytes(StandardCharsets.UTF_8));

        assertEvents(events, List.of(new ServerSentEvent(ServerSentEventField.DATA, "test")));
    }
}
