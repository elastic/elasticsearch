/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
     * Execute a naturally-async action (e.g. to close a shard) but using the current thread so that it completes synchronously, re-throwing
     * any exception that might be passed to its listener.
     */
    public static void executeDirectly(CheckedConsumer<ActionListener<Void>, IOException> action) throws IOException {
        final var closeExceptionRef = new AtomicReference<Exception>();
        ActionListener.run(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                closeExceptionRef.set(e);
            }
        }, action);
        final var closeException = closeExceptionRef.get();
        if (closeException != null) {
            if (closeException instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (closeException instanceof IOException ioException) {
                throw ioException;
            }
            assert false : closeException;
            throw new RuntimeException("unexpected exception on shard close", closeException);
        }
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
