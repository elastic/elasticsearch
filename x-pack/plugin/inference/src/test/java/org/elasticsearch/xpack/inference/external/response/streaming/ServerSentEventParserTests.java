/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ServerSentEventParserTests extends ESTestCase {
    // Shared constants for mixed-terminator tests: the same values appear in both the
    // input byte arrays and the expected ServerSentEvent results.
    private static final String MIXED_TERM_EVENT_TYPE = "user_join";
    private static final String MIXED_TERM_DATA = "{\"username\":\"Alice\"}";

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

    public void testParsingIsIndependentOfNetworkReadBoundaries() {
        var first = "first";
        var second = "second";
        for (var sep : new String[] { "\n", "\r", "\r\n" }) {
            var payload = ("data: " + first + sep + sep + "data: " + second + sep + sep).getBytes(StandardCharsets.UTF_8);
            for (int splitAt = 1; splitAt < payload.length; splitAt++) {
                var parser = new ServerSentEventParser();
                var allEvents = new ArrayDeque<ServerSentEvent>();
                parser.parse(Arrays.copyOfRange(payload, 0, splitAt)).forEach(allEvents::offer);
                parser.parse(Arrays.copyOfRange(payload, splitAt, payload.length)).forEach(allEvents::offer);
                assertEvents(
                    allEvents,
                    List.of(new ServerSentEvent("message", first), new ServerSentEvent("message", second)),
                    "separator=" + sep.replace("\r", "\\r").replace("\n", "\\n") + " splitAt=" + splitAt
                );
            }
        }
    }

    public void testUtf8CharacterCanBeSplitAcrossNetworkReads() {
        // U+1F389 PARTY POPPER — 4 bytes in UTF-8: 0xF0 0x9F 0x8E 0x89
        var emoji = "🎉";
        var payload = ("data: " + emoji + "\n\n").getBytes(StandardCharsets.UTF_8);
        for (int splitAt = 1; splitAt < payload.length; splitAt++) {
            var parser = new ServerSentEventParser();
            var allEvents = new ArrayDeque<ServerSentEvent>();
            parser.parse(Arrays.copyOfRange(payload, 0, splitAt)).forEach(allEvents::offer);
            parser.parse(Arrays.copyOfRange(payload, splitAt, payload.length)).forEach(allEvents::offer);
            assertEvents(allEvents, List.of(new ServerSentEvent("message", emoji)), "splitAt=" + splitAt);
        }
    }

    public void testToolCallIdIsNotLostAcrossReadBoundary() {
        // The event-separator \n\n is split across two network reads
        // so the first \n ends read-1 and the second \n begins read-2.
        // This ensures that events 1 and 2 are not merged.
        var reasoningData = """
            {"id":"1",\
            "choices":[{"delta":{"content":"thinking"},"finish_reason":null,"index":0}],\
            "model":"m","object":"chat.completion.chunk"}\
            """;
        var toolCallIdData = """
            {"id":"2",\
            "choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_abc","type":"function"}]},"finish_reason":null,"index":0}],\
            "model":"m","object":"chat.completion.chunk"}\
            """;
        var argsData = """
            {"id":"3",\
            "choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{}","name":"fn"}}]},"finish_reason":null,"index":0}],\
            "model":"m","object":"chat.completion.chunk"}\
            """;

        // Split so the first \n of the \n\n separator ends the first read
        var firstRead = ("data: " + reasoningData + "\n").getBytes(StandardCharsets.UTF_8);
        var secondRead = ("\ndata: " + toolCallIdData + "\n\ndata: " + argsData + "\n\n").getBytes(StandardCharsets.UTF_8);

        var parser = new ServerSentEventParser();
        var allEvents = new ArrayDeque<ServerSentEvent>();
        parser.parse(firstRead).forEach(allEvents::offer);
        parser.parse(secondRead).forEach(allEvents::offer);

        assertThat(allEvents.size(), equalTo(3));
    }

    /**
     * Verifies that event dispatch is triggered by an <em>empty line</em>, regardless of which
     * terminator ({@code \n}, {@code \r}, or {@code \r\n}) ends each field line.
     *
     * <p>Key WHATWG rule (https://html.spec.whatwg.org/multipage/server-sent-events.html):
     * an LF that is <em>preceded by a CR</em> is the second half of a CRLF pair and does
     * <em>not</em> start a new line. So {@code data: ...\r\n} is one terminated line, not a
     * data line plus a blank line — and therefore does not dispatch an event.
     */
    public void testMixedLineTerminatorsWithinSingleEvent() {
        {
            // Case 1: CRLF field line, then LF data line, then LF blank line → one event dispatched.
            var scenario = ("event: " + MIXED_TERM_EVENT_TYPE + "\r\ndata: " + MIXED_TERM_DATA + "\n\n").getBytes(StandardCharsets.UTF_8);
            var parser = new ServerSentEventParser();
            assertEvents(parser.parse(scenario), List.of(new ServerSentEvent(MIXED_TERM_EVENT_TYPE, MIXED_TERM_DATA)));
        }
        {
            // Case 2: the bytes end with \r\n — that is ONE CRLF terminator for the data line, NOT a
            // data-line CR followed by a blank LF. No blank line arrives, so the event is not dispatched
            // (incomplete trailing event is discarded per the spec).
            var scenario = ("event: " + MIXED_TERM_EVENT_TYPE + "\ndata: " + MIXED_TERM_DATA + "\r\n").getBytes(StandardCharsets.UTF_8);
            var parser = new ServerSentEventParser();
            assertTrue(parser.parse(scenario).isEmpty());
        }
        {
            // Case 3: CR terminates the data line; the second CR is an independent blank line → dispatched.
            // This is the unambiguous contrast to Case 2: two CRs are two separate terminators.
            var scenario = ("event: " + MIXED_TERM_EVENT_TYPE + "\ndata: " + MIXED_TERM_DATA + "\r\r").getBytes(StandardCharsets.UTF_8);
            var parser = new ServerSentEventParser();
            assertEvents(parser.parse(scenario), List.of(new ServerSentEvent(MIXED_TERM_EVENT_TYPE, MIXED_TERM_DATA)));
        }
        {
            // Case 4: CRLF data line followed by a CRLF blank line → one event dispatched.
            var scenario = ("event: " + MIXED_TERM_EVENT_TYPE + "\ndata: " + MIXED_TERM_DATA + "\r\n\r\n").getBytes(StandardCharsets.UTF_8);
            var parser = new ServerSentEventParser();
            assertEvents(parser.parse(scenario), List.of(new ServerSentEvent(MIXED_TERM_EVENT_TYPE, MIXED_TERM_DATA)));
        }
    }

    private void assertEvents(Deque<ServerSentEvent> actualEvents, List<ServerSentEvent> expectedEvents, String context) {
        assertThat(context, actualEvents.size(), equalTo(expectedEvents.size()));
        var expectedEvent = expectedEvents.iterator();
        actualEvents.forEach(event -> assertThat(context, event, equalTo(expectedEvent.next())));
    }
}
