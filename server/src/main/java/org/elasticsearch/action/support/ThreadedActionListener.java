/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * An action listener that wraps another action listener and threading its execution.
 */
public final class ThreadedActionListener<Response> extends ActionListener.Delegating<Response, Response> {

    private final Logger logger;
    private final ThreadPool threadPool;
    private final String executor;
    private final boolean forceExecution;

    public ThreadedActionListener(
        Logger logger,
        ThreadPool threadPool,
        String executor,
        ActionListener<Response> listener,
        boolean forceExecution
    ) {
        super(listener);
        this.logger = logger;
        this.threadPool = threadPool;
        this.executor = executor;
        this.forceExecution = forceExecution;
    }

    @Override
    public void onResponse(final Response response) {
        threadPool.executor(executor).execute(new ActionRunnable<>(delegate) {
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
        threadPool.executor(executor).execute(new AbstractRunnable() {
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
                assert false : e;
                logger.error(() -> "failed to execute failure callback on [" + delegate + "]", e);
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
