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
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
 * Lines are separated by LF, CR, or CRLF.
 * If the line is empty, we do not dispatch the event since we do that automatically. Instead, we discard this event.
 * If the line starts with a colon, we discard this event.
 * If the line contains a colon, we process it into {@link ServerSentEvent} with a non-empty value.
 * If the line does not contain a colon, we process it into {@link ServerSentEvent}with an empty string value.
 * If the line's field is not one of {@link ServerSentEventField}, we discard this event.
 */
public class ServerSentEventParser {
    private static final Pattern END_OF_LINE_REGEX = Pattern.compile("\\n|\\r|\\r\\n");
    private static final String BOM = "\uFEFF";
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

            if (line.isBlank() == false && line.startsWith(":") == false) {
                if (line.contains(":")) {
                    fieldValueEvent(line).ifPresent(collector::offer);
                } else {
                    ServerSentEventField.oneOf(line).map(ServerSentEvent::new).ifPresent(collector::offer);
                }
            }
        }

        // we can sometimes get bytes for incomplete messages, so we save them for the next onNext invocation
        // if we get an onComplete before we clear this cache, we follow the spec to treat it as an incomplete event and discard it since
        // it was not followed by a blank line
        previousTokens = lines[lines.length - 1];
        return collector;
    }

    private Optional<ServerSentEvent> fieldValueEvent(String lineWithColon) {
        var firstColon = lineWithColon.indexOf(":");
        var fieldStr = lineWithColon.substring(0, firstColon);
        var serverSentField = ServerSentEventField.oneOf(fieldStr);

        if ((firstColon + 1) != lineWithColon.length()) {
            var value = lineWithColon.substring(firstColon + 1);
            if (value.equals(" ") == false) {
                var trimmedValue = value.charAt(0) == ' ' ? value.substring(1) : value;
                return serverSentField.map(field -> new ServerSentEvent(field, trimmedValue));
            }
        }

        // if we have "data:" or "data: ", treat it like a no-value line
        return serverSentField.map(ServerSentEvent::new);
    }

}
