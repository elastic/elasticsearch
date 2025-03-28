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
import java.util.regex.Pattern;

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
    private static final Pattern END_OF_LINE_REGEX = Pattern.compile("\\r\\n|\\n|\\r");
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
        var lines = END_OF_LINE_REGEX.split(body, -1); // -1 because we actually want trailing empty strings

        var collector = new ArrayDeque<ServerSentEvent>(lines.length);
        for (var i = 0; i < lines.length - 1; i++) {
            var line = lines[i].replace(BOM, "");

            if (line.isBlank()) {
                eventBuffer.dispatch().ifPresent(collector::offer);
            } else if (line.startsWith(":") == false) {
                if (line.contains(":")) {
                    fieldValueEvent(line);
                } else if (DATA_FIELD.equals(line.toLowerCase(Locale.ROOT))) {
                    eventBuffer.data("");
                }
            }
        }

        // we can sometimes get bytes for incomplete messages, so we save them for the next onNext invocation
        // if we get an onComplete before we clear this cache, we follow the spec to treat it as an incomplete event and discard it since
        // it was not followed by a blank line
        previousTokens = lines[lines.length - 1];
        return collector;
    }

    private void fieldValueEvent(String lineWithColon) {
        var firstColon = lineWithColon.indexOf(":");
        var fieldStr = lineWithColon.substring(0, firstColon).toLowerCase(Locale.ROOT);

        var value = lineWithColon.substring(firstColon + 1);
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
            if (appendLineFeed) {
                this.data.append(LINE_FEED);
            } else {
                // the next time we add data, append line feed
                appendLineFeed = true;
            }
            this.data.append(data);
        }

        private Optional<ServerSentEvent> dispatch() {
            var dataValue = data.toString();

            // if the data buffer is empty, reset without dispatching
            if (dataValue.isEmpty()) {
                reset();
                return Optional.empty();
            }

            // if the type buffer is not empty, set that as the type, else default to message
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
