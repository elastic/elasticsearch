/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.rest.RestRequest;

import java.util.List;
import java.util.Map;

/**
 * A slim interface for precursors to HTTP requests, which doesn't expose access to the request's body,
 * because it's not available yet.
 */
public interface HttpPreRequest {

    /**
     * Returns the HTTP method used in the HTTP request.
     *
     * @return the {@link RestRequest.Method} used in the request
     * @throws IllegalArgumentException if the HTTP method is invalid
     */
    RestRequest.Method method();

    /**
     * The uri with the query string.
     */
    String uri();

    /**
     * The uri without the query string.
     */
    default String rawPath() {
        String uri = uri();
        final int index = uri.indexOf('?');
        if (index >= 0) {
            return uri.substring(0, index);
        } else {
            final int index2 = uri.indexOf('#');
            if (index2 >= 0) {
                return uri.substring(0, index2);
            } else {
                return uri;
            }
        }
    }

    /**
     * Get all of the headers and values associated with the HTTP headers.
     * Modifications of this map are not supported.
     */
    Map<String, List<String>> getHeaders();

    default String header(String name) {
        List<String> values = getHeaders().get(name);
        if (values != null && values.isEmpty() == false) {
            return values.get(0);
        }
        return null;
    }

}
