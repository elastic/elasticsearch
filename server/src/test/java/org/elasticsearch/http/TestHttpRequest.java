/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

class TestHttpRequest implements HttpRequest {

    private final Supplier<HttpVersion> version;
    private final RestRequest.Method method;
    private final String uri;
    private final HashMap<String, List<String>> headers = new HashMap<>();

    TestHttpRequest(Supplier<HttpVersion> versionSupplier, RestRequest.Method method, String uri) {
        this.version = versionSupplier;
        this.method = method;
        this.uri = uri;
    }

    TestHttpRequest(HttpVersion version, RestRequest.Method method, String uri) {
        this(() -> version, method, uri);
    }

    @Override
    public RestRequest.Method method() {
        return method;
    }

    @Override
    public String uri() {
        return uri;
    }

    @Override
    public BytesReference content() {
        return BytesArray.EMPTY;
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    @Override
    public List<String> strictCookies() {
        return Arrays.asList("cookie", "cookie2");
    }

    @Override
    public HttpVersion protocolVersion() {
        return version.get();
    }

    @Override
    public HttpRequest removeHeader(String header) {
        throw new UnsupportedOperationException("Do not support removing header on test request.");
    }

    @Override
    public HttpResponse createResponse(RestStatus status, BytesReference content) {
        return new TestHttpResponse(status, content);
    }

    @Override
    public void release() {
    }

    @Override
    public HttpRequest releaseAndCopy() {
        return this;
    }

    @Override
    public Exception getInboundException() {
        return null;
    }
}
