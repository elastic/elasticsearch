/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.nio.cors.NioCorsConfig;
import org.elasticsearch.http.nio.cors.NioCorsHandler;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class NioHttpChannel extends AbstractRestChannel {

    private final BigArrays bigArrays;
    private final int sequence;
    private final NioCorsConfig corsConfig;
    private final ThreadContext threadContext;
    private final FullHttpRequest nettyRequest;
    private final NioSocketChannel nioChannel;
    private final boolean resetCookies;

    NioHttpChannel(NioSocketChannel nioChannel, BigArrays bigArrays, NioHttpRequest request, int sequence,
                   HttpHandlingSettings settings, NioCorsConfig corsConfig, ThreadContext threadContext) {
        super(request, settings.getDetailedErrorsEnabled());
        this.nioChannel = nioChannel;
        this.bigArrays = bigArrays;
        this.sequence = sequence;
        this.corsConfig = corsConfig;
        this.threadContext = threadContext;
        this.nettyRequest = request.getRequest();
        this.resetCookies = settings.isResetCookies();
    }

    @Override
    public void sendResponse(RestResponse response) {
        // if the response object was created upstream, then use it;
        // otherwise, create a new one
        ByteBuf buffer = ByteBufUtils.toByteBuf(response.content());
        final FullHttpResponse resp;
        if (HttpMethod.HEAD.equals(nettyRequest.method())) {
            resp = newResponse(Unpooled.EMPTY_BUFFER);
        } else {
            resp = newResponse(buffer);
        }
        resp.setStatus(getStatus(response.status()));

        NioCorsHandler.setCorsResponseHeaders(nettyRequest, resp, corsConfig);

        String opaque = nettyRequest.headers().get("X-Opaque-Id");
        if (opaque != null) {
            setHeaderField(resp, "X-Opaque-Id", opaque);
        }

        // Add all custom headers
        addCustomHeaders(resp, response.getHeaders());
        addCustomHeaders(resp, threadContext.getResponseHeaders());

        ArrayList<Releasable> toClose = new ArrayList<>(3);

        boolean success = false;
        try {
            // If our response doesn't specify a content-type header, set one
            setHeaderField(resp, HttpHeaderNames.CONTENT_TYPE.toString(), response.contentType(), false);
            // If our response has no content-length, calculate and set one
            setHeaderField(resp, HttpHeaderNames.CONTENT_LENGTH.toString(), String.valueOf(buffer.readableBytes()), false);

            addCookies(resp);

            BytesReference content = response.content();
            if (content instanceof Releasable) {
                toClose.add((Releasable) content);
            }
            BytesStreamOutput bytesStreamOutput = bytesOutputOrNull();
            if (bytesStreamOutput instanceof ReleasableBytesStreamOutput) {
                toClose.add((Releasable) bytesStreamOutput);
            }

            if (isCloseConnection()) {
                toClose.add(nioChannel::close);
            }

            BiConsumer<Void, Exception> listener = (aVoid, ex) -> Releasables.close(toClose);
            nioChannel.getContext().sendMessage(new NioHttpResponse(sequence, resp), listener);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(toClose);
            }
        }
    }

    @Override
    protected BytesStreamOutput newBytesOutput() {
        return new ReleasableBytesStreamOutput(bigArrays);
    }

    private void setHeaderField(HttpResponse resp, String headerField, String value) {
        setHeaderField(resp, headerField, value, true);
    }

    private void setHeaderField(HttpResponse resp, String headerField, String value, boolean override) {
        if (override || !resp.headers().contains(headerField)) {
            resp.headers().add(headerField, value);
        }
    }

    private void addCookies(HttpResponse resp) {
        if (resetCookies) {
            String cookieString = nettyRequest.headers().get(HttpHeaderNames.COOKIE);
            if (cookieString != null) {
                Set<Cookie> cookies = ServerCookieDecoder.STRICT.decode(cookieString);
                if (!cookies.isEmpty()) {
                    // Reset the cookies if necessary.
                    resp.headers().set(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookies));
                }
            }
        }
    }

    private void addCustomHeaders(HttpResponse response, Map<String, List<String>> customHeaders) {
        if (customHeaders != null) {
            for (Map.Entry<String, List<String>> headerEntry : customHeaders.entrySet()) {
                for (String headerValue : headerEntry.getValue()) {
                    setHeaderField(response, headerEntry.getKey(), headerValue);
                }
            }
        }
    }

    // Create a new {@link HttpResponse} to transmit the response for the netty request.
    private FullHttpResponse newResponse(ByteBuf buffer) {
        final boolean http10 = isHttp10();
        final boolean close = isCloseConnection();
        // Build the response object.
        final HttpResponseStatus status = HttpResponseStatus.OK; // default to initialize
        final FullHttpResponse response;
        if (http10) {
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_0, status, buffer);
            if (!close) {
                response.headers().add(HttpHeaderNames.CONNECTION, "Keep-Alive");
            }
        } else {
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, buffer);
        }
        return response;
    }

    // Determine if the request protocol version is HTTP 1.0
    private boolean isHttp10() {
        return nettyRequest.protocolVersion().equals(HttpVersion.HTTP_1_0);
    }

    // Determine if the request connection should be closed on completion.
    private boolean isCloseConnection() {
        final boolean http10 = isHttp10();
        return HttpHeaderValues.CLOSE.contentEqualsIgnoreCase(nettyRequest.headers().get(HttpHeaderNames.CONNECTION)) ||
            (http10 && !HttpHeaderValues.KEEP_ALIVE.contentEqualsIgnoreCase(nettyRequest.headers().get(HttpHeaderNames.CONNECTION)));
    }

    private static Map<RestStatus, HttpResponseStatus> MAP;

    static {
        EnumMap<RestStatus, HttpResponseStatus> map = new EnumMap<>(RestStatus.class);
        map.put(RestStatus.CONTINUE, HttpResponseStatus.CONTINUE);
        map.put(RestStatus.SWITCHING_PROTOCOLS, HttpResponseStatus.SWITCHING_PROTOCOLS);
        map.put(RestStatus.OK, HttpResponseStatus.OK);
        map.put(RestStatus.CREATED, HttpResponseStatus.CREATED);
        map.put(RestStatus.ACCEPTED, HttpResponseStatus.ACCEPTED);
        map.put(RestStatus.NON_AUTHORITATIVE_INFORMATION, HttpResponseStatus.NON_AUTHORITATIVE_INFORMATION);
        map.put(RestStatus.NO_CONTENT, HttpResponseStatus.NO_CONTENT);
        map.put(RestStatus.RESET_CONTENT, HttpResponseStatus.RESET_CONTENT);
        map.put(RestStatus.PARTIAL_CONTENT, HttpResponseStatus.PARTIAL_CONTENT);
        map.put(RestStatus.MULTI_STATUS, HttpResponseStatus.INTERNAL_SERVER_ERROR); // no status for this??
        map.put(RestStatus.MULTIPLE_CHOICES, HttpResponseStatus.MULTIPLE_CHOICES);
        map.put(RestStatus.MOVED_PERMANENTLY, HttpResponseStatus.MOVED_PERMANENTLY);
        map.put(RestStatus.FOUND, HttpResponseStatus.FOUND);
        map.put(RestStatus.SEE_OTHER, HttpResponseStatus.SEE_OTHER);
        map.put(RestStatus.NOT_MODIFIED, HttpResponseStatus.NOT_MODIFIED);
        map.put(RestStatus.USE_PROXY, HttpResponseStatus.USE_PROXY);
        map.put(RestStatus.TEMPORARY_REDIRECT, HttpResponseStatus.TEMPORARY_REDIRECT);
        map.put(RestStatus.BAD_REQUEST, HttpResponseStatus.BAD_REQUEST);
        map.put(RestStatus.UNAUTHORIZED, HttpResponseStatus.UNAUTHORIZED);
        map.put(RestStatus.PAYMENT_REQUIRED, HttpResponseStatus.PAYMENT_REQUIRED);
        map.put(RestStatus.FORBIDDEN, HttpResponseStatus.FORBIDDEN);
        map.put(RestStatus.NOT_FOUND, HttpResponseStatus.NOT_FOUND);
        map.put(RestStatus.METHOD_NOT_ALLOWED, HttpResponseStatus.METHOD_NOT_ALLOWED);
        map.put(RestStatus.NOT_ACCEPTABLE, HttpResponseStatus.NOT_ACCEPTABLE);
        map.put(RestStatus.PROXY_AUTHENTICATION, HttpResponseStatus.PROXY_AUTHENTICATION_REQUIRED);
        map.put(RestStatus.REQUEST_TIMEOUT, HttpResponseStatus.REQUEST_TIMEOUT);
        map.put(RestStatus.CONFLICT, HttpResponseStatus.CONFLICT);
        map.put(RestStatus.GONE, HttpResponseStatus.GONE);
        map.put(RestStatus.LENGTH_REQUIRED, HttpResponseStatus.LENGTH_REQUIRED);
        map.put(RestStatus.PRECONDITION_FAILED, HttpResponseStatus.PRECONDITION_FAILED);
        map.put(RestStatus.REQUEST_ENTITY_TOO_LARGE, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
        map.put(RestStatus.REQUEST_URI_TOO_LONG, HttpResponseStatus.REQUEST_URI_TOO_LONG);
        map.put(RestStatus.UNSUPPORTED_MEDIA_TYPE, HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE);
        map.put(RestStatus.REQUESTED_RANGE_NOT_SATISFIED, HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
        map.put(RestStatus.EXPECTATION_FAILED, HttpResponseStatus.EXPECTATION_FAILED);
        map.put(RestStatus.UNPROCESSABLE_ENTITY, HttpResponseStatus.BAD_REQUEST);
        map.put(RestStatus.LOCKED, HttpResponseStatus.BAD_REQUEST);
        map.put(RestStatus.FAILED_DEPENDENCY, HttpResponseStatus.BAD_REQUEST);
        map.put(RestStatus.TOO_MANY_REQUESTS,  HttpResponseStatus.TOO_MANY_REQUESTS);
        map.put(RestStatus.INTERNAL_SERVER_ERROR, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        map.put(RestStatus.NOT_IMPLEMENTED, HttpResponseStatus.NOT_IMPLEMENTED);
        map.put(RestStatus.BAD_GATEWAY, HttpResponseStatus.BAD_GATEWAY);
        map.put(RestStatus.SERVICE_UNAVAILABLE, HttpResponseStatus.SERVICE_UNAVAILABLE);
        map.put(RestStatus.GATEWAY_TIMEOUT, HttpResponseStatus.GATEWAY_TIMEOUT);
        map.put(RestStatus.HTTP_VERSION_NOT_SUPPORTED, HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED);
        MAP = Collections.unmodifiableMap(map);
    }

    private static HttpResponseStatus getStatus(RestStatus status) {
        return MAP.getOrDefault(status, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
}
