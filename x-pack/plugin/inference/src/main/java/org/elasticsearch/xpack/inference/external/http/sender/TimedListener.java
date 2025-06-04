/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * Provides a way to set a timeout on the listener. If the time expires, the original listener's
 * {@link ActionListener#onFailure(Exception)} is called with an error indicating there was a timeout.
 *
 * @param <Response> the type of the value that is passed in {@link ActionListener#onResponse(Object)}
 */
public class TimedListener<Response> {

    private final ActionListener<Response> listenerWithTimeout;
    private final AtomicBoolean completed = new AtomicBoolean();

    public TimedListener(@Nullable TimeValue timeout, ActionListener<Response> listener, ThreadPool threadPool) {
        listenerWithTimeout = getListener(Objects.requireNonNull(listener), timeout, Objects.requireNonNull(threadPool));
    }

    private ActionListener<Response> getListener(
        ActionListener<Response> origListener,
        @Nullable TimeValue timeout,
        ThreadPool threadPool
    ) {
        ActionListener<Response> notificationListener = ActionListener.wrap(result -> {
            completed.set(true);
            origListener.onResponse(result);
        }, e -> {
            completed.set(true);
            origListener.onFailure(e);
        });

        if (timeout == null) {
            return notificationListener;
        }

        return ListenerTimeouts.wrapWithTimeout(
            threadPool,
            timeout,
            threadPool.executor(UTILITY_THREAD_POOL_NAME),
            notificationListener,
            (ignored) -> notificationListener.onFailure(
                new ElasticsearchStatusException(Strings.format("Request timed out after [%s]", timeout), RestStatus.REQUEST_TIMEOUT)
            )
        );
    }

    public boolean hasCompleted() {
        return completed.get();
    }

    public ActionListener<Response> getListener() {
        return listenerWithTimeout;
    }
}
