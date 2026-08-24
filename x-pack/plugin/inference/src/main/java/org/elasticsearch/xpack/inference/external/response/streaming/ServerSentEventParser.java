/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;
import java.util.Optional;

/**
 * Parses a server-sent event stream according to the WHATWG SSE specification:
 * <a href="https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation">WHATWG SSE spec</a>
 *
 * <h2>Implemented behaviors</h2>
 * <ul>
 *   <li>Lines are separated by LF ({@code \n}), CR ({@code \r}), or CRLF ({@code \r\n}).</li>
 *   <li>A leading U+FEFF BYTE ORDER MARK on the very first processed line is stripped (once only).</li>
 *   <li>Lines starting with {@code :} are comments and are discarded.</li>
 *   <li>Lines containing {@code :} are split into field name and value; exactly one leading space
 *       in the value is removed if present.</li>
 *   <li>Multiple {@code data:} lines in one event are concatenated with a LF between them.</li>
 *   <li>An empty line dispatches the accumulated event if at least one {@code data} field was seen
 *       (even if that field had an empty value).</li>
 * </ul>
 *
 * <h2>Retained deviations from the WHATWG spec (knowingly kept)</h2>
 * <ul>
 *   <li><b>Case-insensitive field names</b> — field names are lower-cased before matching;
 *       the spec matches them case-sensitively, so {@code DATA:} should be an unknown field.</li>
 *   <li><b>No-colon lines only honored for {@code data}</b> — a bare {@code data} line
 *       (no colon) is treated as {@code data} with an empty value; any other bare field name is
 *       ignored. The spec treats any no-colon line as that field with an empty value.</li>
 *   <li><b>{@code id} and {@code retry} are not implemented</b> — intentionally omitted;
 *       we do not use reconnect state.</li>
 * </ul>
 */
public class ServerSentEventParser {
    private static final String BOM = "\uFEFF";
    private static final String TYPE_FIELD = "event";
    private static final String DATA_FIELD = "data";
    private final EventBuffer eventBuffer = new EventBuffer();
    private final ByteArrayOutputStream pendingLine = new ByteArrayOutputStream();
    private boolean previousByteWasCarriageReturn;
    private boolean firstLine = true;

    /**
     * Parses the given bytes and returns any complete SSE events found so far.
     * Incomplete lines (no terminator yet) are retained internally and completed on the next call.
     * An incomplete trailing line present when the stream ends is discarded per the SSE spec.
     * <p>
     * Successive calls for a single stream are serial but may execute on different threads.
     * {@code synchronized} provides cross-thread visibility of the carry-over state
     * ({@link #pendingLine}, {@link #previousByteWasCarriageReturn}, and the {@link EventBuffer})
     * without requiring {@code volatile} on each field, and additionally guards against
     * accidental concurrent access if the calling infrastructure ever changes.
     */
    public synchronized Deque<ServerSentEvent> parse(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return new ArrayDeque<>(0);
        }

        var collector = new ArrayDeque<ServerSentEvent>();
        for (byte b : bytes) {
            if (previousByteWasCarriageReturn) {
                previousByteWasCarriageReturn = false;
                if (b == '\n') {
                    // CRLF: the CR already flushed the line; swallow the LF
                    continue;
                }
            }
            if (b == '\r') {
                processPendingLine(collector);
                previousByteWasCarriageReturn = true;
            } else if (b == '\n') {
                processPendingLine(collector);
            } else {
                pendingLine.write(b);
            }
        }
        return collector;
    }

    private void processPendingLine(Deque<ServerSentEvent> collector) {
        var line = pendingLine.toString(StandardCharsets.UTF_8);
        pendingLine.reset();
        // Spec: "one leading U+FEFF BYTE ORDER MARK character must be ignored if any are present" — once, at stream start.
        if (firstLine) {
            firstLine = false;
            if (line.startsWith(BOM)) {
                line = line.substring(BOM.length());
            }
        }

        if (line.isEmpty()) {
            eventBuffer.dispatch().ifPresent(collector::offer);
        } else if (line.startsWith(":") == false) {
            if (line.contains(":")) {
                fieldValueEvent(line);
            } else if (DATA_FIELD.equals(line.toLowerCase(Locale.ROOT))) {
                eventBuffer.data("");
            }
        }
    }

    private void fieldValueEvent(String lineWithColon) {
        var firstColon = lineWithColon.indexOf(":");
        var fieldStr = lineWithColon.substring(0, firstColon).toLowerCase(Locale.ROOT);

        var value = lineWithColon.substring(firstColon + 1);
        // "If value starts with a U+0020 SPACE character, remove it from value."
        var trimmedValue = value.length() > 0 && value.charAt(0) == ' ' ? value.substring(1) : value;

        if (DATA_FIELD.equals(fieldStr)) {
            eventBuffer.data(trimmedValue);
        } else if (TYPE_FIELD.equals(fieldStr)) {
            eventBuffer.type(trimmedValue);
        }
    }

    private static class EventBuffer {
        private static final char LINE_FEED = '\n';
        private static final String MESSAGE = "message";
        private StringBuilder type = new StringBuilder();
        private StringBuilder data = new StringBuilder();
        // True once at least one data field has been seen; used by dispatch() to distinguish
        // "no data field" (drop event) from "data field with empty value" (dispatch empty event).
        private boolean dataFieldSeen = false;

        private void type(String type) {
            this.type.append(type);
        }

        private void data(String data) {
            // Spec: append value then a LF for every data field; we join lazily (LF before 2nd+ value),
            // which yields the identical buffer after the trailing-LF strip in dispatch().
            if (dataFieldSeen) {
                this.data.append(LINE_FEED);
            }
            dataFieldSeen = true;
            this.data.append(data);
        }

        private Optional<ServerSentEvent> dispatch() {
            // Spec dispatch step 2: "If the data buffer is an empty string, return." runs before step 3's
            // trailing-LF strip. A lone empty data field appends "" then LF → buffer "\n" → not empty →
            // dispatches with data "". We emulate this correctly by checking dataFieldSeen: if no data
            // field was ever seen the buffer is also "" but we must NOT dispatch.
            if (dataFieldSeen == false) {
                reset();
                return Optional.empty();
            }
            var dataValue = data.toString();

            // "Initialize event's type attribute to "message""
            // "If the event type buffer has a value other than the empty string,
            // change the type of the newly created event to equal the value of the event type buffer."
            var typeValue = type.toString();
            typeValue = typeValue.isBlank() ? MESSAGE : typeValue;

            reset();

            return Optional.of(new ServerSentEvent(typeValue, dataValue));
        }

        private void reset() {
            type = new StringBuilder();
            data = new StringBuilder();
            dataFieldSeen = false;
        }
    }

}
