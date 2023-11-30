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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.batching.TransactionHandler;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

class RequestTask2<K> implements Task<K> {

    private final AtomicBoolean finished = new AtomicBoolean();
    private final TransactionHandler<K> handler;
    private final List<String> input;
    private final ActionListener<HttpResult> listener;

    RequestTask2(
        TransactionHandler<K> handler,
        List<String> input,
        @Nullable TimeValue timeout,
        ThreadPool threadPool,
        ActionListener<HttpResult> listener
    ) {
        this.handler = Objects.requireNonNull(handler);
        this.input = Objects.requireNonNull(input);
        this.listener = getListener(Objects.requireNonNull(listener), timeout, Objects.requireNonNull(threadPool));
    }

    private ActionListener<HttpResult> getListener(
        ActionListener<HttpResult> origListener,
        @Nullable TimeValue timeout,
        ThreadPool threadPool
    ) {
        ActionListener<HttpResult> notificationListener = ActionListener.wrap(result -> {
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
            this::onTimeout
        );
    }

    // TODO if this times out technically the retrying sender could still be retrying. We should devise a way
    // to cancel the retryer task
    private void onTimeout(ActionListener<HttpResult> listener) {
        finished.set(true);
        listener.onFailure(new ElasticsearchTimeoutException("Request timed out waiting to be sent"));
    }

    @Override
    public boolean hasFinished() {
        return finished.get();
    }

    @Override
    public boolean shouldShutdown() {
        return false;
    }

    @Override
    public List<String> input() {
        return input;
    }

    @Override
    public ActionListener<HttpResult> listener() {
        return listener;
    }

    @Override
    public void onRejection(Exception e) {
        listener.onFailure(e);
    }

    @Override
    public TransactionHandler<K> handler() {
        return handler;
    }
}
