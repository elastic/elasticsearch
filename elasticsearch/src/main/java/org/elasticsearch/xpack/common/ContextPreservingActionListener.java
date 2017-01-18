/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.function.Supplier;

/**
 * Restores the given {@link org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext}
 * once the listener is invoked
 */
public final class ContextPreservingActionListener<R> implements ActionListener<R> {

    private final ActionListener<R> delegate;
    private final Supplier<ThreadContext.StoredContext> context;

    public ContextPreservingActionListener(Supplier<ThreadContext.StoredContext> contextSupplier, ActionListener<R> delegate) {
        this.delegate = delegate;
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
}
