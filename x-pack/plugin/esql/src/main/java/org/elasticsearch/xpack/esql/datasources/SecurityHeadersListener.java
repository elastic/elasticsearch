/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.HashMap;
import java.util.Map;

/**
 * Runs a listener on an empty {@link ThreadContext} that carries only the security headers and the project id header captured when
 * the listener was created.
 * <p>
 * Unlike {@code ContextPreservingActionListener}, which restores a full copy of the issuing thread's context, this carries the
 * security principal and the project it belongs to, and nothing else. The completing thread's own context is replaced entirely,
 * including the tracing and project headers that {@link ThreadContext#stashContext()} would keep, and nothing written while the
 * listener runs outlives it. A continuation therefore cannot combine the issuing user with another query's headers. Response
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
        Map<String, String> headers = new HashMap<>(ClientHelper.filterSecurityHeaders(threadContext.getHeaders()));
        String projectId = threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
        if (projectId != null) {
            headers.put(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        }
        return new SecurityHeadersListener<>(threadContext, Map.copyOf(headers), delegate);
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
        ThreadContext.StoredContext restore = threadContext.newEmptyContext();
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
