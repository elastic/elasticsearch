/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.TransportRequestOptions;

/**
 * A generic action. Should strive to make it a singleton.
 */
public class ActionType<Response extends ActionResponse> {

    private final String name;
    private final Writeable.Reader<Response> responseReader;

    /**
     * @param name The name of the action, must be unique across actions.
     * @param responseReader A reader for the response type
     */
    public ActionType(String name, Writeable.Reader<Response> responseReader) {
        this.name = name;
        this.responseReader = responseReader;
    }

    /**
     * The name of the action. Must be unique across actions.
     */
    public String name() {
        return this.name;
    }

    /**
     * Get a reader that can create a new instance of the class from a {@link org.elasticsearch.common.io.stream.StreamInput}
     */
    public Writeable.Reader<Response> getResponseReader() {
        return responseReader;
    }

    /**
     * Optional request options for the action.
     */
    public TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ActionType && name.equals(((ActionType<?>) o).name());
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
