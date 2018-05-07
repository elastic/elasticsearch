/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Exceptions {

    private Exceptions() {
    }

    /**
     * Throws {@link ElasticsearchSecurityException} with status
     * {@link RestStatus#UNAUTHORIZED}
     *
     * @param msg
     *            Error message
     * @param args
     *            Error message arguments
     * @return {@link ElasticsearchSecurityException}
     */
    public static ElasticsearchSecurityException authenticationError(String msg, Object... args) {
        return authenticationError(new HashMap<>(), msg, null, args);
    }

    /**
     * Throws {@link ElasticsearchSecurityException} with status
     * {@link RestStatus#UNAUTHORIZED}
     *
     * @param responseHeaders
     *            Headers to be added to exception.
     * @param msg
     *            Error message
     * @param cause
     *            with specific cause
     * @param args
     *            Error message arguments
     * @return {@link ElasticsearchSecurityException}
     */
    public static ElasticsearchSecurityException authenticationError(Map<String, String[]> responseHeaders, String msg,
            Throwable cause, Object... args) {
        ElasticsearchSecurityException e = new ElasticsearchSecurityException(msg, RestStatus.UNAUTHORIZED, cause, args);
        for (Entry<String, String[]> headerEntry : responseHeaders.entrySet()) {
            e.addHeader(headerEntry.getKey(), headerEntry.getValue());
        }
        return e;
    }

    /**
     * Throws {@link ElasticsearchSecurityException} with status
     * {@link RestStatus#FORBIDDEN}
     *
     * @param msg
     *            Error message
     * @param args
     *            Error message arguments
     * @return {@link ElasticsearchSecurityException}
     */
    public static ElasticsearchSecurityException authorizationError(String msg, Object... args) {
        return new ElasticsearchSecurityException(msg, RestStatus.FORBIDDEN, args);
    }
}
