/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportException;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code FailureCollector} is responsible for collecting exceptions that occur in the compute engine.
 * The collected exceptions are categorized into task-cancelled and non-task-cancelled exceptions.
 * To limit memory usage, this class collects only the first 10 exceptions in each category by default.
 * When returning the accumulated failure to the caller, this class prefers non-task-cancelled exceptions
 * over task-cancelled ones as they are more useful for diagnosing issues.
 */
public final class FailureCollector {
    private final Queue<Exception> cancelledExceptions = ConcurrentCollections.newQueue();
    private final AtomicInteger cancelledExceptionsCount = new AtomicInteger();

    private final Queue<Exception> nonCancelledExceptions = ConcurrentCollections.newQueue();
    private final AtomicInteger nonCancelledExceptionsCount = new AtomicInteger();

    private final int maxExceptions;
    private volatile boolean hasFailure = false;
    private Exception finalFailure = null;

    public FailureCollector() {
        this(10);
    }

    public FailureCollector(int maxExceptions) {
        if (maxExceptions <= 0) {
            throw new IllegalArgumentException("maxExceptions must be at least one");
        }
        this.maxExceptions = maxExceptions;
    }

    public void unwrapAndCollect(Exception originEx) {
        final Exception e = originEx instanceof TransportException
            ? (originEx.getCause() instanceof Exception cause ? cause : new ElasticsearchException(originEx.getCause()))
            : originEx;
        if (ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null) {
            if (cancelledExceptionsCount.incrementAndGet() <= maxExceptions) {
                cancelledExceptions.add(e);
            }
        } else {
            if (nonCancelledExceptionsCount.incrementAndGet() <= maxExceptions) {
                nonCancelledExceptions.add(e);
            }
        }
        hasFailure = true;
    }

    /**
     * @return {@code true} if any failure has been collected, {@code false} otherwise
     */
    public boolean hasFailure() {
        return hasFailure;
    }

    /**
     * Returns the accumulated failure, preferring non-task-cancelled exceptions over task-cancelled ones.
     * Once this method builds the failure, incoming failures are discarded.
     *
     * @return the accumulated failure, or {@code null} if no failure has been collected
     */
    public Exception getFailure() {
        if (hasFailure == false) {
            return null;
        }
        synchronized (this) {
            if (finalFailure == null) {
                finalFailure = buildFailure();
            }
            return finalFailure;
        }
    }

    private Exception buildFailure() {
        assert hasFailure;
        assert Thread.holdsLock(this);
        int total = 0;
        Exception first = null;
        for (var exceptions : List.of(nonCancelledExceptions, cancelledExceptions)) {
            for (Exception e : exceptions) {
                if (first == null) {
                    first = e;
                    total++;
                } else if (first != e) {
                    first.addSuppressed(e);
                    total++;
                }
                if (total >= maxExceptions) {
                    return first;
                }
            }
        }
        assert first != null;
        return first;
    }
}
