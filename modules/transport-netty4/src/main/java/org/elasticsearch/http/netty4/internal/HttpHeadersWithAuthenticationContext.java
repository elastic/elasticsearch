/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4.internal;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Objects;

/**
 * {@link HttpHeaders} implementation that carries along the {@link ThreadContext.StoredContext} iff
 * the HTTP headers have been authenticated successfully.
 */
public final class HttpHeadersWithAuthenticationContext extends DefaultHttpHeaders {

    public final SetOnce<ThreadContext.StoredContext> authenticationContextSetOnce;

    public HttpHeadersWithAuthenticationContext(HttpHeaders httpHeaders) {
        this(httpHeaders, new SetOnce<>());
    }

    private HttpHeadersWithAuthenticationContext(
        HttpHeaders httpHeaders,
        SetOnce<ThreadContext.StoredContext> authenticationContextSetOnce
    ) {
        // the constructor implements the same logic as HttpHeaders#copy
        super();
        set(httpHeaders);
        this.authenticationContextSetOnce = authenticationContextSetOnce;
    }

    private HttpHeadersWithAuthenticationContext(HttpHeaders httpHeaders, ThreadContext.StoredContext authenticationContext) {
        this(httpHeaders);
        if (authenticationContext != null) {
            setAuthenticationContext(authenticationContext);
        }
    }

    /**
     * Must be called at most once in order to mark the http headers as successfully authenticated.
     * The intent of the {@link ThreadContext.StoredContext} parameter is to associate the resulting
     * thread context post authentication, that will later be restored when dispatching the request.
     */
    public void setAuthenticationContext(ThreadContext.StoredContext authenticationContext) {
        this.authenticationContextSetOnce.set(Objects.requireNonNull(authenticationContext));
    }

    @Override
    public HttpHeaders copy() {
        // copy the headers but also STILL CARRY the same validation result
        return new HttpHeadersWithAuthenticationContext(super.copy(), authenticationContextSetOnce.get());
    }
}
