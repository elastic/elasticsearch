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

import java.util.concurrent.ExecutorService;

/**
 * An action listener that wraps another action listener and dispatches its completion to an executor.
 */
public final class ThreadedActionListener<Response> extends ActionListener.Delegating<Response, Response> {

    private static final Logger logger = LogManager.getLogger(ThreadedActionListener.class);

    private final ExecutorService executor;
    private final boolean forceExecution;

    public ThreadedActionListener(ExecutorService executor, ActionListener<Response> listener) {
        this(executor, false, listener);
    }

    public ThreadedActionListener(ExecutorService executor, boolean forceExecution, ActionListener<Response> listener) {
        super(listener);
        this.forceExecution = forceExecution;
        this.executor = executor;
    }

    @Override
    public void onResponse(final Response response) {
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
                return ThreadedActionListener.this + "/onResponse";
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
            public void onRejection(Exception e2) {
                e.addSuppressed(e2);
                try {
                    delegate.onFailure(e);
                } catch (Exception e3) {
                    e.addSuppressed(e3);
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> "failed to execute failure callback on [" + ThreadedActionListener.this + "]", e);
                assert false : e;
            }

            @Override
            public String toString() {
                return ThreadedActionListener.this + "/onFailure";
            }
        });
    }

    @Override
    public String toString() {
        return "ThreadedActionListener[" + executor + "/" + delegate + "]";
    }
}
