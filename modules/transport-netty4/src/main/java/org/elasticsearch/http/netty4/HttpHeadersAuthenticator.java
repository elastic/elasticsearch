/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.BasicHttpRequest;
import org.elasticsearch.rest.RestRequest;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class HttpHeadersAuthenticator {

    private final TriConsumer<BasicHttpRequest, Channel, ThreadContext> populateThreadContext;
    private final BiConsumer<BasicHttpRequest, ActionListener<Void>> authenticate;
    private final ThreadContext threadContext;

    public HttpHeadersAuthenticator(
        TriConsumer<BasicHttpRequest, Channel, ThreadContext> populateThreadContext,
        BiConsumer<BasicHttpRequest, ActionListener<Void>> authenticate,
        ThreadContext threadContext
    ) {
        this.populateThreadContext = populateThreadContext;
        this.authenticate = authenticate;
        this.threadContext = threadContext;
    }

    public static final HttpHeadersAuthenticator NOOP = new HttpHeadersAuthenticator(null, null, null) {
        @Override
        public DefaultHttpRequest wrapNewMessage(DefaultHttpRequest decodedNewMessage) {
            return decodedNewMessage;
        }

        @Override
        public void authenticateMessage(HttpRequest request, Channel channel, ActionListener<Void> listener) {
            listener.onResponse(null);
        }
    };

    public DefaultHttpRequest wrapNewMessage(DefaultHttpRequest decodedNewMessage) {
        HttpHeadersWithAuthenticationContext httpHeadersWithAuthenticationContext = new HttpHeadersWithAuthenticationContext(
            decodedNewMessage.headers()
        );
        return new DefaultHttpRequest(
            decodedNewMessage.protocolVersion(),
            decodedNewMessage.method(),
            decodedNewMessage.uri(),
            httpHeadersWithAuthenticationContext
        );
    }

    public void authenticateMessage(HttpRequest request, Channel channel, ActionListener<Void> listener) {
        assert request.headers() instanceof HttpHeadersAuthenticator.HttpHeadersWithAuthenticationContext;
        final BasicHttpRequest requestToAuthenticate = wrapToBasicHttpRequest(request);
        final Supplier<ThreadContext.StoredContext> emptyContext = threadContext.wrapRestorable(threadContext.newStoredContext());
        final ActionListener<Void> contextPreservingListener = new ContextPreservingActionListener<>(emptyContext, listener);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            populateThreadContext.apply(requestToAuthenticate, channel, threadContext);
            authenticate.accept(requestToAuthenticate, ActionListener.wrap(ignored -> {
                final ThreadContext.StoredContext authenticatedContext = threadContext.newStoredContext();
                ((HttpHeadersWithAuthenticationContext) request.headers()).markAuthenticationSucceeded(authenticatedContext);
                contextPreservingListener.onResponse(null);
            }, e -> {
                ((HttpHeadersWithAuthenticationContext) request.headers()).markAuthenticationFailed(e);
                contextPreservingListener.onFailure(e);
            }));
        }
    }

    private static BasicHttpRequest wrapToBasicHttpRequest(HttpRequest request) {
        return new BasicHttpRequest() {

            @Override
            public RestRequest.Method method() {
                return Netty4HttpRequest.translateRequestMethod(request.method());
            }

            @Override
            public String uri() {
                return request.uri();
            }

            @Override
            public Map<String, List<String>> getHeaders() {
                return Netty4HttpRequest.wrapHttpHeaders(request.headers());
            }
        };
    }

    public static ThreadContext.StoredContext extractAuthenticationContext(org.elasticsearch.http.HttpRequest request) {
        HttpHeadersWithAuthenticationContext authenticatedHeaders = unwrapHeadersAuthenticationContext(request);
        return authenticatedHeaders != null ? authenticatedHeaders.authenticatedContext.get() : null;
    }

    public static Exception extractAuthenticationException(org.elasticsearch.http.HttpRequest request) {
        HttpHeadersWithAuthenticationContext authenticatedHeaders = unwrapHeadersAuthenticationContext(request);
        return authenticatedHeaders != null ? authenticatedHeaders.authenticationException.get() : null;
    }

    private static HttpHeadersWithAuthenticationContext unwrapHeadersAuthenticationContext(org.elasticsearch.http.HttpRequest request) {
        if (request instanceof Netty4HttpRequest == false) {
            return null;
        }
        if (((Netty4HttpRequest) request).getNettyRequest().headers() instanceof HttpHeadersWithAuthenticationContext == false) {
            return null;
        }
        return ((HttpHeadersWithAuthenticationContext) (((Netty4HttpRequest) request).getNettyRequest().headers()));
    }

    public static class HttpHeadersWithAuthenticationContext extends DefaultHttpHeaders {

        public final SetOnce<ThreadContext.StoredContext> authenticatedContext = new SetOnce<>();
        public final SetOnce<Exception> authenticationException = new SetOnce<>();

        public HttpHeadersWithAuthenticationContext(HttpHeaders httpHeaders) {
            // the constructor implements the same logic as HttpHeaders#copy
            super();
            set(httpHeaders);
        }

        public void markAuthenticationSucceeded(ThreadContext.StoredContext authenticatedContext) {
            this.authenticatedContext.set(authenticatedContext);
            this.authenticationException.set(null);
        }

        public void markAuthenticationFailed(Exception exception) {
            this.authenticatedContext.set(null);
            this.authenticationException.set(exception);
        }
    }
}
