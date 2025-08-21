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
import org.elasticsearch.core.RefCounted;

import java.util.concurrent.Executor;

/**
 * Same as {@link ThreadedActionListener} but for {@link RefCounted} types. Makes sure to increment ref-count by one before forking
 * to another thread and decrementing after the forked task completes.
 */
public final class RefCountAwareThreadedActionListener<Response extends RefCounted> extends AbstractThreadedActionListener<Response> {

    public RefCountAwareThreadedActionListener(Executor executor, ActionListener<Response> delegate) {
        super(executor, false, delegate);
    }

    @Override
    public void onResponse(final Response response) {
        response.mustIncRef();
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
                return RefCountAwareThreadedActionListener.this + "/onResponse";
            }

            @Override
            public void onAfter() {
                response.decRef();
            }
        });
    }
}
