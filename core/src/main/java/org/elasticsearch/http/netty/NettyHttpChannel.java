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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.netty.ReleaseChannelFutureListener;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.netty.pipelining.OrderedDownstreamChannelEvent;
import org.elasticsearch.http.netty.pipelining.OrderedUpstreamMessageEvent;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.support.RestUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.elasticsearch.http.netty.NettyHttpServerTransport.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;

/**
 *
 */
public class NettyHttpChannel extends HttpChannel {

    private final NettyHttpServerTransport transport;
    private final Channel channel;
    private final org.jboss.netty.handler.codec.http.HttpRequest nettyRequest;
    private OrderedUpstreamMessageEvent orderedUpstreamMessageEvent = null;
    private Pattern corsPattern;

    public NettyHttpChannel(NettyHttpServerTransport transport, NettyHttpRequest request, Pattern corsPattern, boolean detailedErrorsEnabled) {
        super(request, detailedErrorsEnabled);
        this.transport = transport;
        this.channel = request.getChannel();
        this.nettyRequest = request.request();
        this.corsPattern = corsPattern;
    }

    public NettyHttpChannel(NettyHttpServerTransport transport, NettyHttpRequest request, Pattern corsPattern, OrderedUpstreamMessageEvent orderedUpstreamMessageEvent, boolean detailedErrorsEnabled) {
        this(transport, request, corsPattern, detailedErrorsEnabled);
        this.orderedUpstreamMessageEvent = orderedUpstreamMessageEvent;
    }

    @Override
    public BytesStreamOutput newBytesOutput() {
        return new ReleasableBytesStreamOutput(transport.bigArrays);
    }


    @Override
    public void sendResponse(RestResponse response) {
        // Decide whether to close the connection or not.
        boolean http10 = nettyRequest.getProtocolVersion().equals(HttpVersion.HTTP_1_0);
        boolean close =
                HttpHeaders.Values.CLOSE.equalsIgnoreCase(nettyRequest.headers().get(HttpHeaders.Names.CONNECTION)) ||
                        (http10 && !HttpHeaders.Values.KEEP_ALIVE.equalsIgnoreCase(nettyRequest.headers().get(HttpHeaders.Names.CONNECTION)));

        // Build the response object.
        HttpResponseStatus status = getStatus(response.status());
        org.jboss.netty.handler.codec.http.HttpResponse resp;
        if (http10) {
            resp = new DefaultHttpResponse(HttpVersion.HTTP_1_0, status);
            if (!close) {
                resp.headers().add(HttpHeaders.Names.CONNECTION, "Keep-Alive");
            }
        } else {
            resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
        }
        if (RestUtils.isBrowser(nettyRequest.headers().get(USER_AGENT))) {
            if (transport.settings().getAsBoolean(SETTING_CORS_ENABLED, false)) {
                String originHeader = request.header(ORIGIN);
                if (!Strings.isNullOrEmpty(originHeader)) {
                    if (corsPattern == null) {
                        String allowedOrigins = transport.settings().get(SETTING_CORS_ALLOW_ORIGIN, null);
                        if (!Strings.isNullOrEmpty(allowedOrigins)) {
                            resp.headers().add(ACCESS_CONTROL_ALLOW_ORIGIN, allowedOrigins);
                        }
                    } else {
                        resp.headers().add(ACCESS_CONTROL_ALLOW_ORIGIN, corsPattern.matcher(originHeader).matches() ? originHeader : "null");
                    }
                }
                if (nettyRequest.getMethod() == HttpMethod.OPTIONS) {
                    // Allow Ajax requests based on the CORS "preflight" request
                    resp.headers().add(ACCESS_CONTROL_MAX_AGE, transport.settings().getAsInt(SETTING_CORS_MAX_AGE, 1728000));
                    resp.headers().add(ACCESS_CONTROL_ALLOW_METHODS, transport.settings().get(SETTING_CORS_ALLOW_METHODS, "OPTIONS, HEAD, GET, POST, PUT, DELETE"));
                    resp.headers().add(ACCESS_CONTROL_ALLOW_HEADERS, transport.settings().get(SETTING_CORS_ALLOW_HEADERS, "X-Requested-With, Content-Type, Content-Length"));
                }

                if (transport.settings().getAsBoolean(SETTING_CORS_ALLOW_CREDENTIALS, false)) {
                    resp.headers().add(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
                }
            }
        }

        String opaque = nettyRequest.headers().get("X-Opaque-Id");
        if (opaque != null) {
            resp.headers().add("X-Opaque-Id", opaque);
        }

        // Add all custom headers
        Map<String, List<String>> customHeaders = response.getHeaders();
        if (customHeaders != null) {
            for (Map.Entry<String, List<String>> headerEntry : customHeaders.entrySet()) {
                for (String headerValue : headerEntry.getValue()) {
                    resp.headers().add(headerEntry.getKey(), headerValue);
                }
            }
        }

        BytesReference content = response.content();
        ChannelBuffer buffer;
        boolean addedReleaseListener = false;
        try {
            buffer = content.toChannelBuffer();
            resp.setContent(buffer);

            // If our response doesn't specify a content-type header, set one
            if (!resp.headers().contains(HttpHeaders.Names.CONTENT_TYPE)) {
                resp.headers().add(HttpHeaders.Names.CONTENT_TYPE, response.contentType());
            }

            // If our response has no content-length, calculate and set one
            if (!resp.headers().contains(HttpHeaders.Names.CONTENT_LENGTH)) {
                resp.headers().add(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buffer.readableBytes()));
            }

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
                        resp.headers().add(HttpHeaders.Names.SET_COOKIE, cookieEncoder.encode());
                    }
                }
            }

            ChannelFuture future;

            if (orderedUpstreamMessageEvent != null) {
                OrderedDownstreamChannelEvent downstreamChannelEvent = new OrderedDownstreamChannelEvent(orderedUpstreamMessageEvent, 0, true, resp);
                future = downstreamChannelEvent.getFuture();
                channel.getPipeline().sendDownstream(downstreamChannelEvent);
            } else {
                future = channel.write(resp);
            }

            if (content instanceof Releasable) {
                future.addListener(new ReleaseChannelFutureListener((Releasable) content));
                addedReleaseListener = true;
            }

            if (close) {
                future.addListener(ChannelFutureListener.CLOSE);
            }

        } finally {
            if (!addedReleaseListener && content instanceof Releasable) {
                ((Releasable) content).close();
            }
        }
    }

    private static final HttpResponseStatus TOO_MANY_REQUESTS = new HttpResponseStatus(429, "Too Many Requests");

    private HttpResponseStatus getStatus(RestStatus status) {
        switch (status) {
            case CONTINUE:
                return HttpResponseStatus.CONTINUE;
            case SWITCHING_PROTOCOLS:
                return HttpResponseStatus.SWITCHING_PROTOCOLS;
            case OK:
                return HttpResponseStatus.OK;
            case CREATED:
                return HttpResponseStatus.CREATED;
            case ACCEPTED:
                return HttpResponseStatus.ACCEPTED;
            case NON_AUTHORITATIVE_INFORMATION:
                return HttpResponseStatus.NON_AUTHORITATIVE_INFORMATION;
            case NO_CONTENT:
                return HttpResponseStatus.NO_CONTENT;
            case RESET_CONTENT:
                return HttpResponseStatus.RESET_CONTENT;
            case PARTIAL_CONTENT:
                return HttpResponseStatus.PARTIAL_CONTENT;
            case MULTI_STATUS:
                // no status for this??
                return HttpResponseStatus.INTERNAL_SERVER_ERROR;
            case MULTIPLE_CHOICES:
                return HttpResponseStatus.MULTIPLE_CHOICES;
            case MOVED_PERMANENTLY:
                return HttpResponseStatus.MOVED_PERMANENTLY;
            case FOUND:
                return HttpResponseStatus.FOUND;
            case SEE_OTHER:
                return HttpResponseStatus.SEE_OTHER;
            case NOT_MODIFIED:
                return HttpResponseStatus.NOT_MODIFIED;
            case USE_PROXY:
                return HttpResponseStatus.USE_PROXY;
            case TEMPORARY_REDIRECT:
                return HttpResponseStatus.TEMPORARY_REDIRECT;
            case BAD_REQUEST:
                return HttpResponseStatus.BAD_REQUEST;
            case UNAUTHORIZED:
                return HttpResponseStatus.UNAUTHORIZED;
            case PAYMENT_REQUIRED:
                return HttpResponseStatus.PAYMENT_REQUIRED;
            case FORBIDDEN:
                return HttpResponseStatus.FORBIDDEN;
            case NOT_FOUND:
                return HttpResponseStatus.NOT_FOUND;
            case METHOD_NOT_ALLOWED:
                return HttpResponseStatus.METHOD_NOT_ALLOWED;
            case NOT_ACCEPTABLE:
                return HttpResponseStatus.NOT_ACCEPTABLE;
            case PROXY_AUTHENTICATION:
                return HttpResponseStatus.PROXY_AUTHENTICATION_REQUIRED;
            case REQUEST_TIMEOUT:
                return HttpResponseStatus.REQUEST_TIMEOUT;
            case CONFLICT:
                return HttpResponseStatus.CONFLICT;
            case GONE:
                return HttpResponseStatus.GONE;
            case LENGTH_REQUIRED:
                return HttpResponseStatus.LENGTH_REQUIRED;
            case PRECONDITION_FAILED:
                return HttpResponseStatus.PRECONDITION_FAILED;
            case REQUEST_ENTITY_TOO_LARGE:
                return HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
            case REQUEST_URI_TOO_LONG:
                return HttpResponseStatus.REQUEST_URI_TOO_LONG;
            case UNSUPPORTED_MEDIA_TYPE:
                return HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE;
            case REQUESTED_RANGE_NOT_SATISFIED:
                return HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
            case EXPECTATION_FAILED:
                return HttpResponseStatus.EXPECTATION_FAILED;
            case UNPROCESSABLE_ENTITY:
                return HttpResponseStatus.BAD_REQUEST;
            case LOCKED:
                return HttpResponseStatus.BAD_REQUEST;
            case FAILED_DEPENDENCY:
                return HttpResponseStatus.BAD_REQUEST;
            case TOO_MANY_REQUESTS:
                return TOO_MANY_REQUESTS;
            case INTERNAL_SERVER_ERROR:
                return HttpResponseStatus.INTERNAL_SERVER_ERROR;
            case NOT_IMPLEMENTED:
                return HttpResponseStatus.NOT_IMPLEMENTED;
            case BAD_GATEWAY:
                return HttpResponseStatus.BAD_GATEWAY;
            case SERVICE_UNAVAILABLE:
                return HttpResponseStatus.SERVICE_UNAVAILABLE;
            case GATEWAY_TIMEOUT:
                return HttpResponseStatus.GATEWAY_TIMEOUT;
            case HTTP_VERSION_NOT_SUPPORTED:
                return HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED;
            default:
                return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }
    }
}
