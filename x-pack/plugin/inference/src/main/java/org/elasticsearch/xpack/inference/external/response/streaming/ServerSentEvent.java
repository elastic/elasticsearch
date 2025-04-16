/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

/**
 * Server-Sent Event message: https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
 */
public record ServerSentEvent(String type, String data) {

    private static final String EMPTY = "";
    private static final String MESSAGE = "message";

    public static ServerSentEvent empty() {
        return new ServerSentEvent(EMPTY, EMPTY);
    }

    public ServerSentEvent(String data) {
        this(MESSAGE, data);
    }

    public ServerSentEvent {
        data = data != null ? data : EMPTY;
        type = type != null && type.isBlank() == false ? type : MESSAGE;
    }

    public boolean hasData() {
        return data.isBlank() == false;
    }
}
