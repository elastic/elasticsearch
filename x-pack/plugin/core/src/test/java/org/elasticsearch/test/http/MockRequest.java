/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test.http;

import org.elasticsearch.core.SuppressForbidden;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Locale;

/**
 * A request parsed by the MockWebServer
 */
public class MockRequest {

    private final String method;
    private final URI uri;
    private final Headers headers;
    private final InetSocketAddress remoteAddress;
    private String body = null;

    @SuppressForbidden(reason = "use http server header class")
    MockRequest(String method, URI uri, com.sun.net.httpserver.Headers headers, InetSocketAddress remoteAddress) {
        this.method = method;
        this.uri = uri;
        this.headers = new Headers(headers);
        this.remoteAddress = remoteAddress;
    }

    /**
     * @return The HTTP method of the incoming request
     */
    public String getMethod() {
        return method;
    }

    /**
     * @return The URI of the incoming request
     */
    public URI getUri() {
        return uri;
    }

    /**
     * @return The specific value of a request header, null if it does not exist
     */
    public String getHeader(String name) {
        return headers.getFirst(name);
    }

    /**
     * @return All headers associated with this request
     */
    public Headers getHeaders() {
        return headers;
    }

    /**
     * @return The body the incoming request had, null if no body was found
     */
    public String getBody() {
        return body;
    }

    /**
     * @return The address of the client
     */
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%s %s", method, uri);
    }

    /**
     * @param body Sets the body of the incoming request
     */
    void setBody(String body) {
        this.body = body;
    }
}
