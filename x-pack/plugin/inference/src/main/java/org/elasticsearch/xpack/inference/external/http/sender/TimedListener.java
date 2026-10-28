/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * Provides a way to set a timeout on the listener. If the time expires, the original listener's
 * {@link ActionListener#onFailure(Exception)} is called with an error indicating there was a timeout.
 * <p>
 * Failing the listener does not by itself stop the work that was started on its behalf (for example an http request waiting for a
 * pooled connection). Callers register {@link #addAbortHandler(Runnable) abort handlers} to release such resources once nobody is
 * waiting for the result anymore.
 *
 * @param <Response> the type of the value that is passed in {@link ActionListener#onResponse(Object)}
 */
public class TimedListener<Response> {
    private static final Logger logger = LogManager.getLogger(TimedListener.class);

    private final ActionListener<Response> listenerWithTimeout;
    private final AtomicBoolean completed = new AtomicBoolean();
    private final SubscribableListener<Boolean> failed = new SubscribableListener<>();

    public TimedListener(@Nullable TimeValue timeout, ActionListener<Response> listener, ThreadPool threadPool, String inferenceId) {
        listenerWithTimeout = getListener(
            Objects.requireNonNull(listener),
            timeout,
            Objects.requireNonNull(threadPool),
            Objects.requireNonNull(inferenceId)
        );
    }

    private ActionListener<Response> getListener(
        ActionListener<Response> origListener,
        @Nullable TimeValue timeout,
        ThreadPool threadPool,
        String inferenceId
    ) {
        ActionListener<Response> notificationListener = ActionListener.notifyOnce(ActionListener.wrap(result -> {
            completed.set(true);
            try {
                origListener.onResponse(result);
            } finally {
                failed.onResponse(false);
            }
        }, e -> {
            completed.set(true);
            try {
                origListener.onFailure(e);
            } finally {
                failed.onResponse(true);
            }
        }));

        if (timeout == null) {
            return notificationListener;
        }

        return ListenerTimeouts.wrapWithTimeout(
            threadPool,
            timeout,
            threadPool.executor(UTILITY_THREAD_POOL_NAME),
            notificationListener,
            (ignored) -> notificationListener.onFailure(timeoutException(timeout, inferenceId))
        );
    }

    public boolean hasCompleted() {
        return completed.get();
    }

    public ActionListener<Response> getListener() {
        return listenerWithTimeout;
    }

    /**
     * Registers an action that aborts work started on behalf of this listener. The action runs once the listener fails (including
     * timing out), or immediately if it has already failed. It is dropped without running if the listener succeeds, because a
     * successful response may still be backed by that work (e.g. a streamed response body).
     *
     * @param abortHandler the action releasing the resources held by the abandoned work, it must be safe to call more than once
     */
    public void addAbortHandler(Runnable abortHandler) {
        Objects.requireNonNull(abortHandler);
        failed.addListener(ActionListener.wrap(hasFailed -> {
            if (hasFailed) {
                abortHandler.run();
            }
        }, e -> logger.warn("Failed to abort the work of a failed inference request", e)));
    }

    /**
     * Creates an {@link ElasticsearchTimeoutException} with a message indicating that the request timed out.
     * @param timeout the timeout that was reached
     * @param inferenceId the id of the inference request that timed out
     * @return an {@link ElasticsearchTimeoutException} indicating a timeout occurred
     */
    public static ElasticsearchTimeoutException timeoutException(TimeValue timeout, String inferenceId) {
        return new ElasticsearchTimeoutException("Request timed out after [{}] for inference id [{}]", timeout, inferenceId);
    }
}
