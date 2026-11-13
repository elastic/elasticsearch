/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.Map;

/**
 * Runs a listener on a clean {@link ThreadContext} that carries only the security headers captured when the listener was created.
 * <p>
 * Unlike {@code ContextPreservingActionListener}, which restores a full copy of the issuing thread's context, this carries the
 * security principal and nothing else. The completing thread's own context, including any stale identity, is stashed away, and
 * nothing written while the listener runs outlives it. A continuation therefore cannot inherit another query's identity. Response
 * headers written while it runs are discarded, so warnings on this path must go through an explicit sink.
 */
final class SecurityHeadersListener<T> implements ActionListener<T> {

    private final ThreadContext threadContext;
    private final Map<String, String> securityHeaders;
    private final ActionListener<T> delegate;

    private SecurityHeadersListener(ThreadContext threadContext, Map<String, String> securityHeaders, ActionListener<T> delegate) {
        this.threadContext = threadContext;
        this.securityHeaders = securityHeaders;
        this.delegate = delegate;
    }

    /** Captures the security headers of the calling thread and wraps {@code delegate} so it resumes under them. */
    static <T> ActionListener<T> wrap(ThreadContext threadContext, ActionListener<T> delegate) {
        return new SecurityHeadersListener<>(
            threadContext,
            Map.copyOf(ClientHelper.filterSecurityHeaders(threadContext.getHeaders())),
            delegate
        );
    }

    @Override
    public void onResponse(T response) {
        try (ThreadContext.StoredContext ignored = install()) {
            delegate.onResponse(response);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try (ThreadContext.StoredContext ignored = install()) {
            delegate.onFailure(e);
        }
    }

    private ThreadContext.StoredContext install() {
        ThreadContext.StoredContext restore = threadContext.stashContext();
        try {
            threadContext.copyHeaders(securityHeaders.entrySet());
            return restore;
        } catch (RuntimeException e) {
            restore.close();
            throw e;
        }
    }

    @Override
    public String toString() {
        return "SecurityHeadersListener{" + delegate + "}";
    }
}
