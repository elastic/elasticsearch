/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;
import java.util.Optional;

/**
 * https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
 * Lines are separated by LF, CR, or CRLF.
 * If the line is empty, we do not dispatch the event since we do that automatically. Instead, we discard this event.
 * If the line starts with a colon, we discard this event.
 * If the line contains a colon, we process it into {@link ServerSentEvent} with a non-empty value.
 * If the line does not contain a colon, we process it into {@link ServerSentEvent}with an empty string value.
 * If the line's field is not one of (data, event), we discard this event. `id` and `retry` are not implemented, because we do not use them
 * and have no plans to use them.
 */
public class ServerSentEventParser {
    private static final String BOM = "\uFEFF";
    private static final String TYPE_FIELD = "event";
    private static final String DATA_FIELD = "data";
    private final EventBuffer eventBuffer = new EventBuffer();
    private volatile String previousTokens = "";

    public Deque<ServerSentEvent> parse(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return new ArrayDeque<>(0);
        }

        var body = previousTokens + new String(bytes, StandardCharsets.UTF_8);
        var lines = body.lines();

        var collector = new ArrayDeque<ServerSentEvent>();
        lines.reduce((previousLine, nextLine) -> {
            var line = previousLine.replace(BOM, "");

            if (line.isEmpty()) {
                eventBuffer.dispatch().ifPresent(collector::offer);
            } else if (line.startsWith(":") == false) {
                if (line.contains(":")) {
                    fieldValueEvent(line);
                } else if (DATA_FIELD.equals(line.toLowerCase(Locale.ROOT))) {
                    eventBuffer.data("");
                }
            }
            return nextLine;
        }).ifPresent(lastLine -> {
            if (lastLine.isEmpty()) {
                // if the last line is an empty line, then we dispatch the event and clear the previousToken cache
                eventBuffer.dispatch().ifPresent(collector::offer);
                previousTokens = "";
            } else {
                // we can sometimes get bytes for incomplete messages, so we save them for the next onNext invocation
                // if we get an onComplete before we clear this cache, we follow the spec to treat it as an incomplete event and discard it
                // since it was not followed by a blank line
                previousTokens = lastLine;
            }
        });
        return collector;
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
        private boolean appendLineFeed = false;

        private void type(String type) {
            this.type.append(type);
        }

        private void data(String data) {
            // "Append the field value to the data buffer, then append a single U+000A LINE FEED (LF) character to the data buffer."
            // But then we're told "If the data buffer's last character is a U+000A LINE FEED (LF) character,
            // then remove the last character from the data buffer."
            // Rather than add + remove for single-line data fields, we only append a LINE FEED on subsequent data lines.
            if (appendLineFeed) {
                this.data.append(LINE_FEED);
            } else {
                appendLineFeed = true;
            }
            this.data.append(data);
        }

        private Optional<ServerSentEvent> dispatch() {
            // "If the data buffer is an empty string, set the data buffer and the event type buffer to the empty string and return."
            // We don't process empty events anywhere, so we just drop the events.
            if (data.isEmpty()) {
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
            appendLineFeed = false;
        }
    }

}
