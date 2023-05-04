/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4.internal;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpHeadersValidationException;
import org.elasticsearch.http.HttpPipelinedRequest;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.netty4.Netty4HttpHeaderValidator;
import org.elasticsearch.http.netty4.Netty4HttpRequest;
import org.elasticsearch.rest.RestRequest;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.http.netty4.Netty4HttpRequest.getHttpHeadersAsMap;
import static org.elasticsearch.http.netty4.Netty4HttpRequest.translateRequestMethod;

/**
 * Provides utilities for hooking into the netty pipeline and authenticate each HTTP request's headers.
 * See also {@link Netty4HttpHeaderValidator}.
 */
public final class HttpHeadersAuthenticatorUtils {

    // utility class
    private HttpHeadersAuthenticatorUtils() {}

    /**
     * Supplies a netty {@code ChannelInboundHandler} that runs the provided {@param validator} on the HTTP request headers.
     * The HTTP headers of the to-be-authenticated {@link HttpRequest} must be wrapped by the special
     * {@link HttpHeadersWithAuthenticationContext}, see {@link #wrapAsMessageWithAuthenticationContext(HttpMessage)}.
     */
    public static Netty4HttpHeaderValidator getValidatorInboundHandler(HttpValidator validator, ThreadContext threadContext) {
        return new Netty4HttpHeaderValidator((httpRequest, channel, listener) -> {
            // make sure authentication only runs on properly wrapped "authenticable" headers implementation
            if (httpRequest.headers() instanceof HttpHeadersWithAuthenticationContext) {
                validator.validate(httpRequest, channel, ActionListener.wrap(aVoid -> {
                    ((HttpHeadersWithAuthenticationContext) httpRequest.headers()).setAuthenticationContext(
                        threadContext.newStoredContext(false)
                    );
                    // a successful authentication needs to signal to the {@link Netty4HttpHeaderValidator} to resume
                    // forwarding the request beyond the headers part
                    listener.onResponse(null);
                }, e -> listener.onFailure(new HttpHeadersValidationException(e))));
            } else {
                // cannot authenticate the request because it's not wrapped correctly, see {@link #wrapAsMessageWithAuthenticationContext}
                listener.onFailure(new IllegalStateException("Cannot authenticate unwrapped requests"));
            }
        }, threadContext);
    }

    /**
     * Given a {@link DefaultHttpRequest} argument, this returns a new {@link DefaultHttpRequest} instance that's identical to the
     * passed-in one, but the headers of the latter can be authenticated, in the sense that the channel handlers returned by
     * {@link #getValidatorInboundHandler(HttpValidator, ThreadContext)} can use this to convey the authentication result context.
     */
    public static HttpMessage wrapAsMessageWithAuthenticationContext(HttpMessage newlyDecodedMessage) {
        assert newlyDecodedMessage instanceof HttpRequest;
        DefaultHttpRequest httpRequest = (DefaultHttpRequest) newlyDecodedMessage;
        HttpHeadersWithAuthenticationContext httpHeadersWithAuthenticationContext = new HttpHeadersWithAuthenticationContext(
            newlyDecodedMessage.headers()
        );
        return new DefaultHttpRequest(
            httpRequest.protocolVersion(),
            httpRequest.method(),
            httpRequest.uri(),
            httpHeadersWithAuthenticationContext
        );
    }

    /**
     * Returns the authentication thread context for the {@param request}.
     */
    public static ThreadContext.StoredContext extractAuthenticationContext(org.elasticsearch.http.HttpRequest request) {
        HttpHeadersWithAuthenticationContext authenticatedHeaders = unwrapAuthenticatedHeaders(request);
        return authenticatedHeaders != null ? authenticatedHeaders.authenticationContextSetOnce.get() : null;
    }

    /**
     * Translates the netty request internal type to a {@link HttpPreRequest} instance that code outside the network plugin has access to.
     */
    public static HttpPreRequest asHttpPreRequest(HttpRequest request) {
        return new HttpPreRequest() {

            @Override
            public RestRequest.Method method() {
                return translateRequestMethod(request.method());
            }

            @Override
            public String uri() {
                return request.uri();
            }

            @Override
            public Map<String, List<String>> getHeaders() {
                return getHttpHeadersAsMap(request.headers());
            }
        };
    }

    private static HttpHeadersWithAuthenticationContext unwrapAuthenticatedHeaders(org.elasticsearch.http.HttpRequest request) {
        if (request instanceof HttpPipelinedRequest) {
            request = ((HttpPipelinedRequest) request).getDelegateRequest();
        }
        if (request instanceof Netty4HttpRequest == false) {
            return null;
        }
        if (((Netty4HttpRequest) request).getNettyRequest().headers() instanceof HttpHeadersWithAuthenticationContext == false) {
            return null;
        }
        return (HttpHeadersWithAuthenticationContext) (((Netty4HttpRequest) request).getNettyRequest().headers());
    }
}
