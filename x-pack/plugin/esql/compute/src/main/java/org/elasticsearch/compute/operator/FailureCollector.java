/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportException;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * {@code FailureCollector} is responsible for collecting exceptions that occur in the compute engine.
 * The collected exceptions are categorized into client (4xx), server (5xx), shard-unavailable errors,
 * and cancellation errors. To limit memory usage, this class collects only the first 10 exceptions in
 * each category by default. When returning the accumulated failures to the caller, this class prefers
 * client (4xx) errors over server (5xx) errors, shard-unavailable errors, and cancellation errors,
 * as they are more useful for diagnosing issues.
 */
public final class FailureCollector {

    private enum Category {
        CLIENT,
        SERVER,
        SHARD_UNAVAILABLE,
        CANCELLATION
    }

    private final Map<Category, Queue<Exception>> categories;
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
        this.categories = new EnumMap<>(Category.class);
        for (Category c : Category.values()) {
            this.categories.put(c, new ArrayBlockingQueue<>(maxExceptions));
        }
    }

    public static Exception unwrapTransportException(TransportException te) {
        final Throwable cause = te.getCause();
        if (cause == null) {
            return te;
        } else if (cause instanceof Exception ex) {
            return ex;
        } else {
            return new ElasticsearchException(cause);
        }
    }

    private static Category getErrorCategory(Exception e) {
        if (ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null) {
            return Category.CANCELLATION;
        } else if (TransportActions.isShardNotAvailableException(e)) {
            return Category.SHARD_UNAVAILABLE;
        } else {
            final int status = ExceptionsHelper.status(e).getStatus();
            if (400 <= status && status < 500) {
                return Category.CLIENT;
            } else {
                return Category.SERVER;
            }
        }
    }

    public void unwrapAndCollect(Exception e) {
        e = e instanceof TransportException te ? unwrapTransportException(te) : e;
        categories.get(getErrorCategory(e)).offer(e);
        hasFailure = true;
    }

    /**
     * @return {@code true} if any failure has been collected, {@code false} otherwise
     */
    public boolean hasFailure() {
        return hasFailure;
    }

    /**
     * Returns the accumulated failure, preferring client (4xx) errors over server (5xx) errors and cancellation errors,
     * as they are more useful for diagnosing issues. Once this method builds the failure, incoming failures are discarded.
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
        Exception first = null;
        int collected = 0;
        for (Category category : List.of(Category.CLIENT, Category.SERVER, Category.SHARD_UNAVAILABLE, Category.CANCELLATION)) {
            if (first != null && category == Category.CANCELLATION) {
                continue; // do not add cancellation errors if other errors present
            }
            for (Exception e : categories.get(category)) {
                if (++collected <= maxExceptions) {
                    if (first == null) {
                        first = e;
                    } else if (first != e) {
                        first.addSuppressed(e);
                    }
                }
            }
        }
        assert first != null;
        return first;
    }
}
