/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test.http;

import org.elasticsearch.common.SuppressForbidden;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A helper class to not leak the internal headers class into our tests
 * Currently setting multiple values for a single header is not supported, as it was not needed yet
 */
@SuppressForbidden(reason = "use http server")
public class Headers {

    final com.sun.net.httpserver.Headers headers;

    /**
     * Creates a class with empty headers
     */
    Headers() {
        this.headers = new com.sun.net.httpserver.Headers();
    }

    /**
     * Creates a class headers from http
     * @param headers The internal sun webserver headers object
     */
    Headers(com.sun.net.httpserver.Headers headers) {
        this.headers = headers;
    }

    /**
     * @param name The name of header
     * @return A list of values for this header
     */
    public List<String> get(String name) {
        return headers.get(name);
    }

    /**
     * Adds a new header to this headers object
     * @param name Name of the header
     * @param value Value of the header
     */
    void add(String name, String value) {
        this.headers.compute(name, (k, v) -> {
            if (v == null) {
                return Collections.singletonList(value);
            } else {
                List<String> list = new ArrayList<>();
                list.addAll(v);
                list.add(value);
                return list;
            }
        });
    }

    /**
     * @param name Name of the header
     * @return Returns the first header value or null if none exists
     */
    String getFirst(String name) {
        return headers.getFirst(name);
    }
}
