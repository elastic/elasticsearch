/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4.internal;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.settings.SecureReleasable;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.List;
import java.util.Objects;

/**
 * {@link HttpHeaders} implementation that carries along the {@link ThreadContext.StoredContext} iff
 * the HTTP headers have been authenticated successfully.
 */
public final class HttpHeadersWithAuthenticationContext extends DefaultHttpHeaders {

    public final SetOnce<ThreadContext.StoredContext> authenticationContextSetOnce;
    private final SetOnce<List<SecureReleasable>> secureReleasablesSetOnce;

    public HttpHeadersWithAuthenticationContext(HttpHeaders httpHeaders) {
        this(httpHeaders, new SetOnce<>(), new SetOnce<>());
    }

    private HttpHeadersWithAuthenticationContext(
        HttpHeaders httpHeaders,
        SetOnce<ThreadContext.StoredContext> authenticationContextSetOnce,
        SetOnce<List<SecureReleasable>> secureReleasablesSetOnce
    ) {
        // the constructor implements the same logic as HttpHeaders#copy
        super();
        set(httpHeaders);
        this.authenticationContextSetOnce = authenticationContextSetOnce;
        this.secureReleasablesSetOnce = secureReleasablesSetOnce;
    }

    private HttpHeadersWithAuthenticationContext(
        HttpHeaders httpHeaders,
        ThreadContext.StoredContext authenticationContext,
        List<SecureReleasable> secureReleasables
    ) {
        this(httpHeaders);
        if (authenticationContext != null) {
            setAuthenticationContext(authenticationContext);
        }
        if (secureReleasables != null && secureReleasables.isEmpty() == false) {
            setSecureReleasables(secureReleasables);
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

    /**
     * Sets the list of {@link SecureReleasable} objects that should be cleaned up after the response is sent.
     */
    public void setSecureReleasables(List<SecureReleasable> secureReleasables) {
        this.secureReleasablesSetOnce.set(Objects.requireNonNull(secureReleasables));
    }

    /**
     * Returns the list of {@link SecureReleasable} objects that should be cleaned up after the response is sent.
     */
    public List<SecureReleasable> getSecureReleasables() {
        List<SecureReleasable> result = secureReleasablesSetOnce.get();
        return result != null ? result : List.of();
    }

    @Override
    public HttpHeaders copy() {
        // copy the headers but also STILL CARRY the same validation result
        return new HttpHeadersWithAuthenticationContext(super.copy(), authenticationContextSetOnce.get(), secureReleasablesSetOnce.get());
    }
}
