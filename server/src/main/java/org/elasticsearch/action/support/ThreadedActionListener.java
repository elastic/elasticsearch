/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;

import java.util.concurrent.Executor;

/**
 * An action listener that wraps another action listener and dispatches its completion to an executor.
 */
public final class ThreadedActionListener<Response> extends AbstractThreadedActionListener<Response> {

    public ThreadedActionListener(Executor executor, ActionListener<Response> delegate) {
        this(executor, false, delegate);
    }

    public ThreadedActionListener(Executor executor, boolean forceExecution, ActionListener<Response> delegate) {
        super(executor, forceExecution, delegate);
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
}
