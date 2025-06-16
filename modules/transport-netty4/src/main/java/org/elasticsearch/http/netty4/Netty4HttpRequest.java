/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Netty4HttpRequest implements HttpRequest {

    private final int sequence;
    private final io.netty.handler.codec.http.HttpRequest nettyRequest;
    private HttpBody content;
    private final Map<String, List<String>> headers;
    private final AtomicBoolean released;
    private final Exception inboundException;
    private final QueryStringDecoder queryStringDecoder;

    public Netty4HttpRequest(int sequence, io.netty.handler.codec.http.HttpRequest nettyRequest, Exception exception) {
        this(sequence, nettyRequest, HttpBody.empty(), new AtomicBoolean(false), exception);
    }

    public Netty4HttpRequest(int sequence, io.netty.handler.codec.http.HttpRequest nettyRequest, HttpBody content) {
        this(sequence, nettyRequest, content, new AtomicBoolean(false), null);
    }

    public Netty4HttpRequest(
        int sequence,
        io.netty.handler.codec.http.HttpRequest nettyRequest,
        HttpBody content,
        AtomicBoolean released,
        Exception inboundException
    ) {
        this.sequence = sequence;
        this.nettyRequest = nettyRequest;
        this.content = content;
        this.headers = getHttpHeadersAsMap(nettyRequest.headers());
        this.released = released;
        this.inboundException = inboundException;
        this.queryStringDecoder = new QueryStringDecoder(nettyRequest.uri());
    }

    @Override
    public RestRequest.Method method() {
        return translateRequestMethod(nettyRequest.method());
    }

    @Override
    public String uri() {
        return nettyRequest.uri();
    }

    @Override
    public String rawPath() {
        return queryStringDecoder.rawPath();
    }

    @Override
    public HttpBody body() {
        assert released.get() == false;
        return content;
    }

    @Override
    public void setBody(HttpBody body) {
        this.content = body.asFull();
    }

    @Override
    public void release() {
        if (released.compareAndSet(false, true)) {
            content.close();
        }
    }

    @Override
    public final Map<String, List<String>> getHeaders() {
        return headers;
    }

    @Override
    public List<String> strictCookies() {
        String cookieString = nettyRequest.headers().get(HttpHeaderNames.COOKIE);
        if (cookieString != null) {
            Set<Cookie> cookies = ServerCookieDecoder.STRICT.decode(cookieString);
            if (cookies.isEmpty() == false) {
                return ServerCookieEncoder.STRICT.encode(cookies);
            }
        }
        return Collections.emptyList();
    }

    @Override
    public HttpVersion protocolVersion() {
        if (nettyRequest.protocolVersion().equals(io.netty.handler.codec.http.HttpVersion.HTTP_1_0)) {
            return HttpVersion.HTTP_1_0;
        } else if (nettyRequest.protocolVersion().equals(io.netty.handler.codec.http.HttpVersion.HTTP_1_1)) {
            return HttpVersion.HTTP_1_1;
        } else {
            throw new IllegalArgumentException("Unexpected http protocol version: " + nettyRequest.protocolVersion());
        }
    }

    @Override
    public HttpRequest removeHeader(String header) {
        HttpHeaders copiedHeadersWithout = nettyRequest.headers().copy();
        copiedHeadersWithout.remove(header);
        var requestWithoutHeader = new DefaultHttpRequest(
            nettyRequest.protocolVersion(),
            nettyRequest.method(),
            nettyRequest.uri(),
            copiedHeadersWithout
        );
        return new Netty4HttpRequest(sequence, requestWithoutHeader, content, released, null);
    }

    @Override
    public Netty4FullHttpResponse createResponse(RestStatus status, BytesReference contentRef) {
        return new Netty4FullHttpResponse(sequence, nettyRequest.protocolVersion(), status, contentRef);
    }

    @Override
    public HttpResponse createResponse(RestStatus status, ChunkedRestResponseBodyPart firstBodyPart) {
        return new Netty4ChunkedHttpResponse(sequence, nettyRequest.protocolVersion(), status, firstBodyPart);
    }

    @Override
    public Exception getInboundException() {
        return inboundException;
    }

    public io.netty.handler.codec.http.HttpRequest getNettyRequest() {
        return nettyRequest;
    }

    public static RestRequest.Method translateRequestMethod(HttpMethod httpMethod) {
        if (httpMethod == HttpMethod.GET) return RestRequest.Method.GET;

        if (httpMethod == HttpMethod.POST) return RestRequest.Method.POST;

        if (httpMethod == HttpMethod.PUT) return RestRequest.Method.PUT;

        if (httpMethod == HttpMethod.DELETE) return RestRequest.Method.DELETE;

        if (httpMethod == HttpMethod.HEAD) {
            return RestRequest.Method.HEAD;
        }

        if (httpMethod == HttpMethod.OPTIONS) {
            return RestRequest.Method.OPTIONS;
        }

        if (httpMethod == HttpMethod.PATCH) {
            return RestRequest.Method.PATCH;
        }

        if (httpMethod == HttpMethod.TRACE) {
            return RestRequest.Method.TRACE;
        }

        if (httpMethod == HttpMethod.CONNECT) {
            return RestRequest.Method.CONNECT;
        }

        throw new IllegalArgumentException("Unexpected http method: " + httpMethod);
    }

    public static Map<String, List<String>> getHttpHeadersAsMap(HttpHeaders httpHeaders) {
        return new HttpHeadersMap(httpHeaders);
    }

    /**
     * A wrapper of {@link HttpHeaders} that implements a map to prevent copying unnecessarily. This class does not support modifications
     * and due to the underlying implementation, it performs case insensitive lookups of key to values.
     *
     * It is important to note that this implementation does have some downsides in that each invocation of the
     * {@link #values()} and {@link #entrySet()} methods will perform a copy of the values in the HttpHeaders rather than returning a
     * view of the underlying values.
     */
    private static class HttpHeadersMap implements Map<String, List<String>> {

        private final HttpHeaders httpHeaders;

        private HttpHeadersMap(HttpHeaders httpHeaders) {
            this.httpHeaders = httpHeaders;
        }

        @Override
        public int size() {
            return httpHeaders.size();
        }

        @Override
        public boolean isEmpty() {
            return httpHeaders.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return key instanceof String && httpHeaders.contains((String) key);
        }

        @Override
        public boolean containsValue(Object value) {
            return value instanceof List && httpHeaders.names().stream().map(httpHeaders::getAll).anyMatch(value::equals);
        }

        @Override
        public List<String> get(Object key) {
            return key instanceof String ? httpHeaders.getAll((String) key) : null;
        }

        @Override
        public List<String> put(String key, List<String> value) {
            throw new UnsupportedOperationException("modifications are not supported");
        }

        @Override
        public List<String> remove(Object key) {
            throw new UnsupportedOperationException("modifications are not supported");
        }

        @Override
        public void putAll(Map<? extends String, ? extends List<String>> m) {
            throw new UnsupportedOperationException("modifications are not supported");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("modifications are not supported");
        }

        @Override
        public Set<String> keySet() {
            return httpHeaders.names();
        }

        @Override
        public Collection<List<String>> values() {
            return httpHeaders.names().stream().map(k -> Collections.unmodifiableList(httpHeaders.getAll(k))).toList();
        }

        @Override
        public Set<Entry<String, List<String>>> entrySet() {
            return httpHeaders.names()
                .stream()
                .map(k -> new AbstractMap.SimpleImmutableEntry<>(k, httpHeaders.getAll(k)))
                .collect(Collectors.toSet());
        }
    }
}
