/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportException;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Collects exceptions occurred in compute, specifically ignores trivial exceptions such as {@link TaskCancelledException},
 * and selects the most appropriate exception to return to the caller.
 * To bound the memory usage, this class only collects the first 10 non-trivial exceptions.
 */
public final class FailureCollector {
    private final AtomicReference<Exception> exceptionRefs = new AtomicReference<>();
    private final Semaphore exceptionPermits = new Semaphore(10);

    public void unwrapAndCollect(Exception originEx) {
        final Exception e = originEx instanceof TransportException
            ? (originEx.getCause() instanceof Exception cause ? cause : new ElasticsearchException(originEx.getCause()))
            : originEx;
        exceptionRefs.getAndUpdate(first -> {
            if (first == null) {
                return e;
            }
            // ignore subsequent TaskCancelledException exceptions as they don't provide useful info.
            if (ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null) {
                return first;
            }
            if (ExceptionsHelper.unwrap(first, TaskCancelledException.class) != null) {
                return e;
            }
            if (ExceptionsHelper.unwrapCause(first) != ExceptionsHelper.unwrapCause(e)) {
                if (exceptionPermits.tryAcquire()) {
                    first.addSuppressed(e);
                }
            }
            return first;
        });
    }

    public Exception get() {
        return exceptionRefs.get();
    }
}
