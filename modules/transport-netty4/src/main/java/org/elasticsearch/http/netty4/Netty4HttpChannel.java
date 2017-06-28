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

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty4.cors.Netty4CorsHandler;
import org.elasticsearch.http.netty4.pipelining.HttpPipelinedRequest;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class Netty4HttpChannel extends AbstractRestChannel {

    private final Netty4HttpServerTransport transport;
    private final Channel channel;
    private final FullHttpRequest nettyRequest;
    private final HttpPipelinedRequest pipelinedRequest;
    private final ThreadContext threadContext;

    /**
     * @param transport             The corresponding <code>NettyHttpServerTransport</code> where this channel belongs to.
     * @param request               The request that is handled by this channel.
     * @param pipelinedRequest      If HTTP pipelining is enabled provide the corresponding pipelined request. May be null if
     *                              HTTP pipelining is disabled.
     * @param detailedErrorsEnabled true iff error messages should include stack traces.
     * @param threadContext         the thread context for the channel
     */
    Netty4HttpChannel(
            final Netty4HttpServerTransport transport,
            final Netty4HttpRequest request,
            final HttpPipelinedRequest pipelinedRequest,
            final boolean detailedErrorsEnabled,
            final ThreadContext threadContext) {
        super(request, detailedErrorsEnabled);
        this.transport = transport;
        this.channel = request.getChannel();
        this.nettyRequest = request.request();
        this.pipelinedRequest = pipelinedRequest;
        this.threadContext = threadContext;
    }

    @Override
    protected BytesStreamOutput newBytesOutput() {
        return new ReleasableBytesStreamOutput(transport.bigArrays);
    }

    @Override
    public void sendResponse(RestResponse response) {
        // if the response object was created upstream, then use it;
        // otherwise, create a new one
        ByteBuf buffer = Netty4Utils.toByteBuf(response.content());
        final FullHttpResponse resp;
        if (HttpMethod.HEAD.equals(nettyRequest.method())) {
            resp = newResponse(Unpooled.EMPTY_BUFFER);
        } else {
            resp = newResponse(buffer);
        }
        resp.setStatus(getStatus(response.status()));

        Netty4CorsHandler.setCorsResponseHeaders(nettyRequest, resp, transport.getCorsConfig());

        String opaque = nettyRequest.headers().get("X-Opaque-Id");
        if (opaque != null) {
            setHeaderField(resp, "X-Opaque-Id", opaque);
        }

        // Add all custom headers
        addCustomHeaders(resp, response.getHeaders());
        addCustomHeaders(resp, threadContext.getResponseHeaders());

        BytesReference content = response.content();
        boolean releaseContent = content instanceof Releasable;
        boolean releaseBytesStreamOutput = bytesOutputOrNull() instanceof ReleasableBytesStreamOutput;
        try {
            // If our response doesn't specify a content-type header, set one
            setHeaderField(resp, HttpHeaderNames.CONTENT_TYPE.toString(), response.contentType(), false);
            // If our response has no content-length, calculate and set one
            setHeaderField(resp, HttpHeaderNames.CONTENT_LENGTH.toString(), String.valueOf(buffer.readableBytes()), false);

            addCookies(resp);

            final ChannelPromise promise = channel.newPromise();

            if (releaseContent) {
                promise.addListener(f -> ((Releasable)content).close());
            }

            if (releaseBytesStreamOutput) {
                promise.addListener(f -> bytesOutputOrNull().close());
            }

            if (isCloseConnection()) {
                promise.addListener(ChannelFutureListener.CLOSE);
            }

            final Object msg;
            if (pipelinedRequest != null) {
                msg = pipelinedRequest.createHttpResponse(resp, promise);
            } else {
                msg = resp;
            }
            channel.writeAndFlush(msg, promise);
            releaseContent = false;
            releaseBytesStreamOutput = false;
        } finally {
            if (releaseContent) {
                ((Releasable) content).close();
            }
            if (releaseBytesStreamOutput) {
                bytesOutputOrNull().close();
            }
            if (pipelinedRequest != null) {
                pipelinedRequest.release();
            }
        }
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
        if (transport.resetCookies) {
            String cookieString = nettyRequest.headers().get(HttpHeaderNames.COOKIE);
            if (cookieString != null) {
                Set<io.netty.handler.codec.http.cookie.Cookie> cookies = ServerCookieDecoder.STRICT.decode(cookieString);
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

    private static final HttpResponseStatus TOO_MANY_REQUESTS = new HttpResponseStatus(429, "Too Many Requests");

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
        map.put(RestStatus.TOO_MANY_REQUESTS, TOO_MANY_REQUESTS);
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
