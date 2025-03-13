/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

/**
 * Server-Sent Event message: https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
 * Messages always contain a {@link ServerSentEventField} and a non-null payload value.
 * When the stream is parsed and there is no value associated with a {@link ServerSentEventField}, an empty-string is set as the value.
 */
public record ServerSentEvent(ServerSentEventField name, String value) {

    private static final String EMPTY = "";

    public ServerSentEvent(ServerSentEventField name) {
        this(name, EMPTY);
    }

    // treat null value as an empty string, don't break parsing
    public ServerSentEvent(ServerSentEventField name, String value) {
        this.name = name;
        this.value = value != null ? value : EMPTY;
    }

    public boolean hasValue() {
        return value.isBlank() == false;
    }
}
