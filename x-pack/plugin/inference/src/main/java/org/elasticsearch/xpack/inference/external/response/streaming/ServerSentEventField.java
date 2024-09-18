/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The named Server-Sent Event fields: https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
 * Unnamed fields are not recognized and ignored.
 */
public enum ServerSentEventField {
    EVENT,
    DATA,
    ID,
    RETRY;

    private static final Set<String> possibleValues = Arrays.stream(values())
        .map(Enum::name)
        .map(name -> name.toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());

    static Optional<ServerSentEventField> oneOf(String name) {
        if (name != null && possibleValues.contains(name.toLowerCase(Locale.ROOT))) {
            return Optional.of(valueOf(name.toUpperCase(Locale.ROOT)));
        } else {
            return Optional.empty();
        }
    }
}
