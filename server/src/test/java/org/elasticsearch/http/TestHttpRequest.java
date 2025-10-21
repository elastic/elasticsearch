/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class TestHttpRequest implements HttpRequest {

    private final Supplier<HttpVersion> version;
    private final RestRequest.Method method;
    private final String uri;
    private final Map<String, List<String>> headers;
    private final HttpBody body;

    public TestHttpRequest(
        Supplier<HttpVersion> versionSupplier,
        RestRequest.Method method,
        String uri,
        Map<String, List<String>> headers,
        HttpBody body
    ) {
        this.version = versionSupplier;
        this.method = method;
        this.uri = uri;
        this.headers = headers;
        this.body = body;
    }

    public TestHttpRequest(RestRequest.Method method, String uri, Map<String, List<String>> headers, HttpBody body) {
        this(() -> HttpVersion.HTTP_1_1, method, uri, headers, body);
    }

    public TestHttpRequest(RestRequest.Method method, String uri, Map<String, List<String>> headers, BytesReference body) {
        this(() -> HttpVersion.HTTP_1_1, method, uri, headers, HttpBody.fromBytesReference(body));
    }

    public TestHttpRequest(Supplier<HttpVersion> versionSupplier, RestRequest.Method method, String uri) {
        this(versionSupplier, method, uri, new HashMap<>(), HttpBody.empty());
    }

    public TestHttpRequest(HttpVersion version, RestRequest.Method method, String uri) {
        this(() -> version, method, uri);
    }

    public TestHttpRequest(HttpVersion version, RestRequest.Method method, String uri, Map<String, List<String>> headers) {
        this(() -> version, method, uri, headers, HttpBody.empty());
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
    public HttpBody body() {
        return body;
    }

    @Override
    public void setBody(HttpBody body) {
        throw new IllegalStateException("not allowed");
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
    public boolean hasContent() {
        return body.isEmpty() == false;
    }

    @Override
    public HttpResponse createResponse(RestStatus status, BytesReference content) {
        return new TestHttpResponse(status, content);
    }

    @Override
    public HttpResponse createResponse(RestStatus status, ChunkedRestResponseBodyPart firstBodyPart) {
        throw new UnsupportedOperationException("chunked responses not supported");
    }

    @Override
    public void release() {}

    @Override
    public Exception getInboundException() {
        return null;
    }
}
