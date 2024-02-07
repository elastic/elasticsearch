/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

class RequestTask extends AbstractRunnable {
    private static final Logger logger = LogManager.getLogger(RequestTask.class);
    private static final Scheduler.Cancellable NOOP_TIMEOUT_HANDLER = createDefaultHandler();

    private final HttpRequest request;
    private final ActionListener<HttpResult> listener;
    private final Scheduler.Cancellable timeoutHandler;
    private final AtomicBoolean notified = new AtomicBoolean();
    private final TimeValue timeout;
    private final Runnable command;

    RequestTask(
        HttpRequest request,
        HttpClient httpClient,
        HttpClientContext context,
        @Nullable TimeValue timeout,
        ThreadPool threadPool,
        ActionListener<HttpResult> listener
    ) {
        this.request = Objects.requireNonNull(request);
        this.listener = Objects.requireNonNull(listener);
        this.timeout = timeout;
        this.timeoutHandler = startTimer(threadPool, timeout);
        this.command = threadPool.getThreadContext()
            .preserveContext(
                new Command(
                    Objects.requireNonNull(httpClient),
                    this.request,
                    Objects.requireNonNull(context),
                    ActionListener.wrap(this::onSuccess, this::onFailure)
                )
            );
    }

    private Scheduler.Cancellable startTimer(ThreadPool threadPool, TimeValue timeout) {
        Objects.requireNonNull(threadPool);

        if (timeout == null) {
            return NOOP_TIMEOUT_HANDLER;
        }

        return threadPool.schedule(this::onTimeout, timeout, threadPool.executor(UTILITY_THREAD_POOL_NAME));
    }

    private void onTimeout() {
        assert timeout != null : "timeout must be defined to use a timeout handler";
        logger.debug(
            () -> format(
                "Request from inference entity id [%s] timed out after [%s] while waiting to be executed",
                request.inferenceEntityId(),
                timeout
            )
        );
        notifyOfResult(
            () -> listener.onFailure(
                new ElasticsearchTimeoutException(format("Request timed out waiting to be executed after [%s]", timeout))
            )
        );
    }

    private void notifyOfResult(Runnable runnable) {
        if (notified.compareAndSet(false, true)) {
            runnable.run();
        }
    }

    @Override
    public void onFailure(Exception e) {
        timeoutHandler.cancel();
        notifyOfResult(() -> listener.onFailure(e));
    }

    @Override
    protected void doRun() {
        try {
            command.run();
        } catch (Exception e) {
            String message = format("Failed while executing request from inference entity id [%s]", request.inferenceEntityId());
            logger.warn(message, e);
            onFailure(new ElasticsearchException(message, e));
        }
    }

    private void onSuccess(HttpResult result) {
        timeoutHandler.cancel();
        notifyOfResult(() -> listener.onResponse(result));
    }

    @Override
    public String toString() {
        return request.inferenceEntityId();
    }

    private static Scheduler.Cancellable createDefaultHandler() {
        return new Scheduler.Cancellable() {
            @Override
            public boolean cancel() {
                return true;
            }

            @Override
            public boolean isCancelled() {
                return true;
            }
        };
    }

    private record Command(
        HttpClient httpClient,
        HttpRequest requestToSend,
        HttpClientContext context,
        ActionListener<HttpResult> resultListener
    ) implements Runnable {

        @Override
        public void run() {
            try {
                httpClient.send(requestToSend, context, resultListener);
            } catch (Exception e) {
                logger.warn(
                    format("Failed to send request from inference entity id [%s] via the http client", requestToSend.inferenceEntityId()),
                    e
                );
                resultListener.onFailure(
                    new ElasticsearchException(
                        format("Failed to send request from inference entity id [%s]", requestToSend.inferenceEntityId()),
                        e
                    )
                );
            }
        }
    }
}
