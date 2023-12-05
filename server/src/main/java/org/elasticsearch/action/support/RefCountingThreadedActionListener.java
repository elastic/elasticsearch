/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.RefCounted;

import java.util.concurrent.Executor;

/**
 * An action listener that wraps another action listener and dispatches its completion to an executor.
 */
public final class RefCountingThreadedActionListener<Response extends RefCounted> implements ActionListener<Response> {

    private static final Logger logger = LogManager.getLogger(RefCountingThreadedActionListener.class);

    private final Executor executor;
    private final ActionListener<Response> delegate;
    private final boolean forceExecution;

    public RefCountingThreadedActionListener(Executor executor, ActionListener<Response> delegate) {
        this(executor, false, delegate);
    }

    public RefCountingThreadedActionListener(Executor executor, boolean forceExecution, ActionListener<Response> delegate) {
        this.forceExecution = forceExecution;
        this.executor = executor;
        this.delegate = delegate;
    }

    @Override
    public void onResponse(final Response response) {
        response.incRef();
        executor.execute(new ActionRunnable<>(delegate) {
            @Override
            public boolean isForceExecution() {
                return forceExecution;
            }

            @Override
            protected void doRun() {
                listener.onResponse(response);
            }

            @Override
            public String toString() {
                return RefCountingThreadedActionListener.this + "/onResponse";
            }

            @Override
            public void onAfter() {
                response.decRef();
            }
        });
    }

    @Override
    public void onFailure(final Exception e) {
        executor.execute(new AbstractRunnable() {
            @Override
            public boolean isForceExecution() {
                return forceExecution;
            }

            @Override
            protected void doRun() {
                delegate.onFailure(e);
            }

            @Override
            public void onRejection(Exception rejectionException) {
                rejectionException.addSuppressed(e);
                try {
                    delegate.onFailure(rejectionException);
                } catch (Exception doubleFailure) {
                    rejectionException.addSuppressed(doubleFailure);
                    onFailure(rejectionException);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> "failed to execute failure callback on [" + RefCountingThreadedActionListener.this + "]", e);
                assert false : e;
            }

            @Override
            public String toString() {
                return RefCountingThreadedActionListener.this + "/onFailure";
            }
        });
    }

    @Override
    public String toString() {
        return "ThreadedActionListener[" + executor + "/" + delegate + "]";
    }
}
