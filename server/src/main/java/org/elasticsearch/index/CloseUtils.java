/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.CheckedConsumer;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utilities to help with closing shards and indices
 */
public class CloseUtils {

    private CloseUtils() {/* no instances */}

    /**
     * Sentinel result value to record success
     */
    private static final Exception SUCCEEDED = new Exception() {
        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    };

    /**
     * Execute a naturally-async action (e.g. to close a shard) but using the current thread so that it completes synchronously, re-throwing
     * any exception that might be passed to its listener.
     */
    public static void executeDirectly(CheckedConsumer<ActionListener<Void>, IOException> action) throws IOException {
        // it's possible to do this with a PlainActionFuture too but extracting the exact Exception is a bit of a pain because of
        // exception-mangling and/or interrupt handling - see #108125
        final var closeExceptionRef = new AtomicReference<Exception>();
        ActionListener.run(ActionListener.assertOnce(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                closeExceptionRef.set(SUCCEEDED);
            }

            @Override
            public void onFailure(Exception e) {
                closeExceptionRef.set(e);
            }
        }), action);
        final var closeException = closeExceptionRef.get();
        if (closeException == SUCCEEDED) {
            return;
        }
        if (closeException instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (closeException instanceof IOException ioException) {
            throw ioException;
        }
        assert false : closeException;
        if (closeException != null) {
            throw new RuntimeException("unexpected exception on shard close", closeException);
        } // else listener not completed, definitely a bug, but throwing something won't help anyone here
    }

    /**
     * Utility shard-close executor for the cases where we close an {@link IndexService} without having created any shards, so we can assert
     * that it's never used.
     */
    public static final Executor NO_SHARDS_CREATED_EXECUTOR = r -> {
        assert false : r;
        r.run(); // just in case we're wrong, in production we need to actually run the task
    };
}
