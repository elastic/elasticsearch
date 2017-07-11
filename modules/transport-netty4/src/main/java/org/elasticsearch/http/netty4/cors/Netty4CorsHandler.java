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

package org.elasticsearch.http.netty4.cors;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.elasticsearch.common.Strings;

import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Handles <a href="http://www.w3.org/TR/cors/">Cross Origin Resource Sharing</a> (CORS) requests.
 * <p>
 * This handler can be configured using a {@link Netty4CorsConfig}, please
 * refer to this class for details about the configuration options available.
 *
 * This code was borrowed from Netty 4 and refactored to work for Elasticsearch's Netty 3 setup.
 */
public class Netty4CorsHandler extends ChannelDuplexHandler {

    public static final String ANY_ORIGIN = "*";
    private static Pattern SCHEME_PATTERN = Pattern.compile("^https?://");

    private final Netty4CorsConfig config;
    private HttpRequest request;

    /**
     * Creates a new instance with the specified {@link Netty4CorsConfig}.
     */
    public Netty4CorsHandler(final Netty4CorsConfig config) {
        if (config == null) {
            throw new NullPointerException();
        }
        this.config = config;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (config.isCorsSupportEnabled() && msg instanceof HttpRequest) {
            request = (HttpRequest) msg;
            if (isPreflightRequest(request)) {
                handlePreflight(ctx, request);
                return;
            }
            if (config.isShortCircuit() && !validateOrigin()) {
                forbidden(ctx, request);
                return;
            }
        }
        ctx.fireChannelRead(msg);
    }

    public static void setCorsResponseHeaders(HttpRequest request, HttpResponse resp, Netty4CorsConfig config) {
        if (!config.isCorsSupportEnabled()) {
            return;
        }
        String originHeader = request.headers().get(HttpHeaderNames.ORIGIN);
        if (!Strings.isNullOrEmpty(originHeader)) {
            final String originHeaderVal;
            if (config.isAnyOriginSupported()) {
                originHeaderVal = ANY_ORIGIN;
            } else if (config.isOriginAllowed(originHeader) || isSameOrigin(originHeader, request.headers().get(HttpHeaderNames.HOST))) {
                originHeaderVal = originHeader;
            } else {
                originHeaderVal = null;
            }
            if (originHeaderVal != null) {
                resp.headers().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, originHeaderVal);
            }
        }
        if (config.isCredentialsAllowed()) {
            resp.headers().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        }
    }

    private void handlePreflight(final ChannelHandlerContext ctx, final HttpRequest request) {
        final HttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.OK, true, true);
        if (setOrigin(response)) {
            setAllowMethods(response);
            setAllowHeaders(response);
            setAllowCredentials(response);
            setMaxAge(response);
            setPreflightHeaders(response);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        } else {
            forbidden(ctx, request);
        }
    }

    private static void forbidden(final ChannelHandlerContext ctx, final HttpRequest request) {
        ctx.writeAndFlush(new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.FORBIDDEN))
            .addListener(ChannelFutureListener.CLOSE);
    }

    private static boolean isSameOrigin(final String origin, final String host) {
        if (Strings.isNullOrEmpty(host) == false) {
            // strip protocol from origin
            final String originDomain = SCHEME_PATTERN.matcher(origin).replaceFirst("");
            if (host.equals(originDomain)) {
                return true;
            }
        }
        return false;
    }

    /**
     * This is a non CORS specification feature which enables the setting of preflight
     * response headers that might be required by intermediaries.
     *
     * @param response the HttpResponse to which the preflight response headers should be added.
     */
    private void setPreflightHeaders(final HttpResponse response) {
        response.headers().add(config.preflightResponseHeaders());
    }

    private boolean setOrigin(final HttpResponse response) {
        final String origin = request.headers().get(HttpHeaderNames.ORIGIN);
        if (!Strings.isNullOrEmpty(origin)) {
            if ("null".equals(origin) && config.isNullOriginAllowed()) {
                setAnyOrigin(response);
                return true;
            }

            if (config.isAnyOriginSupported()) {
                if (config.isCredentialsAllowed()) {
                    echoRequestOrigin(response);
                    setVaryHeader(response);
                } else {
                    setAnyOrigin(response);
                }
                return true;
            }
            if (config.isOriginAllowed(origin)) {
                setOrigin(response, origin);
                setVaryHeader(response);
                return true;
            }
        }
        return false;
    }

    private boolean validateOrigin() {
        if (config.isAnyOriginSupported()) {
            return true;
        }

        final String origin = request.headers().get(HttpHeaderNames.ORIGIN);
        if (Strings.isNullOrEmpty(origin)) {
            // Not a CORS request so we cannot validate it. It may be a non CORS request.
            return true;
        }

        if ("null".equals(origin) && config.isNullOriginAllowed()) {
            return true;
        }

        // if the origin is the same as the host of the request, then allow
        if (isSameOrigin(origin, request.headers().get(HttpHeaderNames.HOST))) {
            return true;
        }

        return config.isOriginAllowed(origin);
    }

    private void echoRequestOrigin(final HttpResponse response) {
        setOrigin(response, request.headers().get(HttpHeaderNames.ORIGIN));
    }

    private static void setVaryHeader(final HttpResponse response) {
        response.headers().set(HttpHeaderNames.VARY, HttpHeaderNames.ORIGIN);
    }

    private static void setAnyOrigin(final HttpResponse response) {
        setOrigin(response, ANY_ORIGIN);
    }

    private static void setOrigin(final HttpResponse response, final String origin) {
        response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }

    private void setAllowCredentials(final HttpResponse response) {
        if (config.isCredentialsAllowed()
            && !response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN).equals(ANY_ORIGIN)) {
            response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        }
    }

    private static boolean isPreflightRequest(final HttpRequest request) {
        final HttpHeaders headers = request.headers();
        return request.method().equals(HttpMethod.OPTIONS) &&
            headers.contains(HttpHeaderNames.ORIGIN) &&
            headers.contains(HttpHeaderNames.ACCESS_CONTROL_REQUEST_METHOD);
    }

    private void setAllowMethods(final HttpResponse response) {
        response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, config.allowedRequestMethods().stream()
            .map(m -> m.name().trim())
            .collect(Collectors.toList()));
    }

    private void setAllowHeaders(final HttpResponse response) {
        response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, config.allowedRequestHeaders());
    }

    private void setMaxAge(final HttpResponse response) {
        response.headers().set(HttpHeaderNames.ACCESS_CONTROL_MAX_AGE, config.maxAge());
    }

}
