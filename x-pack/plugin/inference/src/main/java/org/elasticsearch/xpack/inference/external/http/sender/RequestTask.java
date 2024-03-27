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
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

class RequestTask implements RejectableTask {

    private final AtomicBoolean finished = new AtomicBoolean();
    private final ExecutableRequestCreator requestCreator;
    private final List<String> input;
    private final ActionListener<InferenceServiceResults> listener;

    RequestTask(
        ExecutableRequestCreator requestCreator,
        List<String> input,
        @Nullable TimeValue timeout,
        ThreadPool threadPool,
        ActionListener<InferenceServiceResults> listener
    ) {
        this.requestCreator = Objects.requireNonNull(requestCreator);
        this.input = Objects.requireNonNull(input);
        this.listener = getListener(Objects.requireNonNull(listener), timeout, Objects.requireNonNull(threadPool));
    }

    private ActionListener<InferenceServiceResults> getListener(
        ActionListener<InferenceServiceResults> origListener,
        @Nullable TimeValue timeout,
        ThreadPool threadPool
    ) {
        ActionListener<InferenceServiceResults> notificationListener = ActionListener.wrap(result -> {
            finished.set(true);
            origListener.onResponse(result);
        }, e -> {
            finished.set(true);
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
                new ElasticsearchTimeoutException(Strings.format("Request timed out waiting to be sent after [%s]", timeout))
            )
        );
    }

    @Override
    public boolean hasCompleted() {
        return finished.get();
    }

    @Override
    public Supplier<Boolean> getRequestCompletedFunction() {
        return this::hasCompleted;
    }

    @Override
    public List<String> getInput() {
        return input;
    }

    @Override
    public ActionListener<InferenceServiceResults> getListener() {
        return listener;
    }

    @Override
    public void onRejection(Exception e) {
        listener.onFailure(e);
    }

    @Override
    public ExecutableRequestCreator getRequestCreator() {
        return requestCreator;
    }
}
