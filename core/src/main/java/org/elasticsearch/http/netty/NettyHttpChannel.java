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

package org.elasticsearch.http.netty;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.netty.ReleaseChannelFutureListener;
import org.elasticsearch.http.netty.cors.CorsHandler;
import org.elasticsearch.http.netty.pipelining.OrderedDownstreamChannelEvent;
import org.elasticsearch.http.netty.pipelining.OrderedUpstreamMessageEvent;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.KEEP_ALIVE;

public final class NettyHttpChannel extends AbstractRestChannel {

    private final NettyHttpServerTransport transport;
    private final Channel channel;
    private final org.jboss.netty.handler.codec.http.HttpRequest nettyRequest;
    private final OrderedUpstreamMessageEvent orderedUpstreamMessageEvent;

    /**
     * @param transport                   The corresponding <code>NettyHttpServerTransport</code> where this channel belongs to.
     * @param request                     The request that is handled by this channel.
     * @param orderedUpstreamMessageEvent If HTTP pipelining is enabled provide the corresponding Netty upstream event. May be null if
     *                                    HTTP pipelining is disabled.
     * @param detailedErrorsEnabled       true iff error messages should include stack traces.
     */
    public NettyHttpChannel(NettyHttpServerTransport transport, NettyHttpRequest request,
                            @Nullable OrderedUpstreamMessageEvent orderedUpstreamMessageEvent,
                            boolean detailedErrorsEnabled) {
        super(request, detailedErrorsEnabled);
        this.transport = transport;
        this.channel = request.getChannel();
        this.nettyRequest = request.request();
        this.orderedUpstreamMessageEvent = orderedUpstreamMessageEvent;
    }

    @Override
    public BytesStreamOutput newBytesOutput() {
        return new ReleasableBytesStreamOutput(transport.bigArrays);
    }


    @Override
    public void sendResponse(RestResponse response) {
        // if the response object was created upstream, then use it;
        // otherwise, create a new one
        HttpResponse resp = newResponse();
        resp.setStatus(getStatus(response.status()));

        CorsHandler.setCorsResponseHeaders(nettyRequest, resp, transport.getCorsConfig());

        String opaque = nettyRequest.headers().get("X-Opaque-Id");
        if (opaque != null) {
            setHeaderField(resp, "X-Opaque-Id", opaque);
        }

        // Add all custom headers
        addCustomHeaders(response, resp);

        BytesReference content = response.content();
        ChannelBuffer buffer;
        boolean addedReleaseListener = false;
        try {
            buffer = content.toChannelBuffer();
            resp.setContent(buffer);

            // If our response doesn't specify a content-type header, set one
            setHeaderField(resp, HttpHeaders.Names.CONTENT_TYPE, response.contentType(), false);
            // If our response has no content-length, calculate and set one
            setHeaderField(resp, HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buffer.readableBytes()), false);

            addCookies(resp);

            ChannelFuture future;

            if (orderedUpstreamMessageEvent != null) {
                OrderedDownstreamChannelEvent downstreamChannelEvent =
                    new OrderedDownstreamChannelEvent(orderedUpstreamMessageEvent, 0, true, resp);
                future = downstreamChannelEvent.getFuture();
                channel.getPipeline().sendDownstream(downstreamChannelEvent);
            } else {
                future = channel.write(resp);
            }

            if (content instanceof Releasable) {
                future.addListener(new ReleaseChannelFutureListener((Releasable) content));
                addedReleaseListener = true;
            }

            if (isCloseConnection()) {
                future.addListener(ChannelFutureListener.CLOSE);
            }

        } finally {
            if (!addedReleaseListener && content instanceof Releasable) {
                ((Releasable) content).close();
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
            String cookieString = nettyRequest.headers().get(HttpHeaders.Names.COOKIE);
            if (cookieString != null) {
                CookieDecoder cookieDecoder = new CookieDecoder();
                Set<Cookie> cookies = cookieDecoder.decode(cookieString);
                if (!cookies.isEmpty()) {
                    // Reset the cookies if necessary.
                    CookieEncoder cookieEncoder = new CookieEncoder(true);
                    for (Cookie cookie : cookies) {
                        cookieEncoder.addCookie(cookie);
                    }
                    setHeaderField(resp, HttpHeaders.Names.SET_COOKIE, cookieEncoder.encode());
                }
            }
        }
    }

    private void addCustomHeaders(RestResponse response, HttpResponse resp) {
        Map<String, List<String>> customHeaders = response.getHeaders();
        if (customHeaders != null) {
            for (Map.Entry<String, List<String>> headerEntry : customHeaders.entrySet()) {
                for (String headerValue : headerEntry.getValue()) {
                    setHeaderField(resp, headerEntry.getKey(), headerValue);
                }
            }
        }
    }

    // Determine if the request protocol version is HTTP 1.0
    private boolean isHttp10() {
        return nettyRequest.getProtocolVersion().equals(HttpVersion.HTTP_1_0);
    }

    // Determine if the request connection should be closed on completion.
    private boolean isCloseConnection() {
        final boolean http10 = isHttp10();
        return CLOSE.equalsIgnoreCase(nettyRequest.headers().get(CONNECTION)) ||
                   (http10 && !KEEP_ALIVE.equalsIgnoreCase(nettyRequest.headers().get(CONNECTION)));
    }

    // Create a new {@link HttpResponse} to transmit the response for the netty request.
    private HttpResponse newResponse() {
        final boolean http10 = isHttp10();
        final boolean close = isCloseConnection();
        // Build the response object.
        HttpResponseStatus status = HttpResponseStatus.OK; // default to initialize
        org.jboss.netty.handler.codec.http.HttpResponse resp;
        if (http10) {
            resp = new DefaultHttpResponse(HttpVersion.HTTP_1_0, status);
            if (!close) {
                resp.headers().add(CONNECTION, "Keep-Alive");
            }
        } else {
            resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
        }
        return resp;
    }

    private static final HttpResponseStatus TOO_MANY_REQUESTS = new HttpResponseStatus(429, "Too Many Requests");

    static Map<RestStatus, HttpResponseStatus> MAP;

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
