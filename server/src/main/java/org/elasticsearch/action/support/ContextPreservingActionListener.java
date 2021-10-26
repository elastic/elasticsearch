/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.function.Supplier;

/**
 * Restores the given {@link org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext}
 * once the listener is invoked
 */
public final class ContextPreservingActionListener<R> extends ActionListener.Delegating<R, R> {

    private final Supplier<ThreadContext.StoredContext> context;

    public ContextPreservingActionListener(Supplier<ThreadContext.StoredContext> contextSupplier, ActionListener<R> delegate) {
        super(delegate);
        this.context = contextSupplier;
    }

    @Override
    public void onResponse(R r) {
        try (ThreadContext.StoredContext ignore = context.get()) {
            delegate.onResponse(r);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try (ThreadContext.StoredContext ignore = context.get()) {
            delegate.onFailure(e);
        }
    }

    /**
     * Wraps the provided action listener in a {@link ContextPreservingActionListener} that will
     * also copy the response headers when the {@link ThreadContext.StoredContext} is closed
     */
    public static <R> ContextPreservingActionListener<R> wrapPreservingContext(ActionListener<R> listener, ThreadContext threadContext) {
        return new ContextPreservingActionListener<>(threadContext.newRestorableContext(true), listener);
    }
}
