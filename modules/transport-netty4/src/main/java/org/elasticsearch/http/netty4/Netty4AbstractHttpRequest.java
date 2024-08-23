/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract non-sealed class Netty4AbstractHttpRequest implements Netty4HttpRequest {

    private final io.netty.handler.codec.http.HttpRequest request;
    private final int sequence;

    Netty4AbstractHttpRequest(int sequence, io.netty.handler.codec.http.HttpRequest request) {
        this.request = request;
        this.sequence = sequence;
    }

    @Override
    public RestRequest.Method method() {
        return translateRequestMethod(request.method());
    }

    @Override
    public String uri() {
        return request.uri();
    }

    @Override
    public final Map<String, List<String>> getHeaders() {
        return new Netty4HttpHeadersMap(request.headers());
    }

    @Override
    public HttpRequest removeHeader(String header) {
        request.headers().remove(header);
        return this;
    }

    @Override
    public List<String> strictCookies() {
        String cookieString = request.headers().get(HttpHeaderNames.COOKIE);
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
        if (request.protocolVersion().equals(io.netty.handler.codec.http.HttpVersion.HTTP_1_0)) {
            return HttpRequest.HttpVersion.HTTP_1_0;
        } else if (request.protocolVersion().equals(io.netty.handler.codec.http.HttpVersion.HTTP_1_1)) {
            return HttpRequest.HttpVersion.HTTP_1_1;
        } else {
            throw new IllegalArgumentException("Unexpected http protocol version: " + request.protocolVersion());
        }
    }

    @Override
    public Netty4FullHttpResponse createResponse(RestStatus status, BytesReference contentRef) {
        return new Netty4FullHttpResponse(sequence, request.protocolVersion(), status, contentRef);
    }

    @Override
    public HttpResponse createResponse(RestStatus status, ChunkedRestResponseBodyPart firstBodyPart) {
        return new Netty4ChunkedHttpResponse(sequence, request.protocolVersion(), status, firstBodyPart);
    }

    @Override
    public io.netty.handler.codec.http.HttpRequest nettyRequest() {
        return request;
    }

    @Override
    public int sequence() {
        return sequence;
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
        return new Netty4HttpHeadersMap(httpHeaders);
    }

}
