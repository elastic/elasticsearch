/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.RequestExecutor;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.ServiceComponents;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * A class for providing a more friendly interface for sending an inference request to a 3rd party service.
 */
public class HttpRequestSender implements Sender {

    /**
     * A helper class for constructing a {@link HttpRequestSender}.
     */
    public static class Factory {
        private final HttpRequestSender httpRequestSender;

        public Factory(ServiceComponents serviceComponents, HttpClientManager httpClientManager, ClusterService clusterService) {
            Objects.requireNonNull(serviceComponents);
            Objects.requireNonNull(clusterService);
            Objects.requireNonNull(httpClientManager);

            var requestSender = new RetryingHttpSender(
                httpClientManager.getHttpClient(),
                serviceComponents.throttlerManager(),
                new RetrySettings(serviceComponents.settings(), clusterService),
                serviceComponents.threadPool()
            );

            var startCompleted = new CountDownLatch(1);
            var executorServiceSettings = new RequestExecutorServiceSettings(serviceComponents.settings());
            executorServiceSettings.init(clusterService);

            var service = new RequestExecutorService(
                serviceComponents.threadPool(),
                startCompleted,
                executorServiceSettings,
                requestSender
            );

            httpRequestSender = new HttpRequestSender(
                serviceComponents.threadPool(),
                httpClientManager,
                requestSender,
                service,
                startCompleted
            );
        }

        public Sender createSender() {
            return httpRequestSender;
        }
    }

    private static final TimeValue DEFAULT_STARTUP_TIMEOUT = TimeValue.THIRTY_SECONDS;

    private final ThreadPool threadPool;
    private final HttpClientManager manager;
    private final AtomicBoolean startInitiated = new AtomicBoolean(false);
    private final AtomicBoolean startCompleted = new AtomicBoolean(false);
    private final RequestSender requestSender;
    private final RequestExecutor service;
    private final CountDownLatch startCompletedLatch;
    private final SubscribableListener<Void> startupNotifier = new SubscribableListener<>();

    // Visible for testing
    protected HttpRequestSender(
        ThreadPool threadPool,
        HttpClientManager httpClientManager,
        RequestSender requestSender,
        RequestExecutor service,
        CountDownLatch startCompletedLatch
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.manager = Objects.requireNonNull(httpClientManager);
        this.requestSender = Objects.requireNonNull(requestSender);
        this.service = Objects.requireNonNull(service);
        this.startCompletedLatch = Objects.requireNonNull(startCompletedLatch);
    }

    /**
     * Start various internal services asynchronously. This is required before sending requests.
     * All callers (including concurrent ones) are notified via {@code listener} when startup completes
     * or fails, without blocking any thread pool threads while waiting.
     *
     * @param timeout the maximum time to wait for startup to complete; if {@code null} a default {@link #DEFAULT_STARTUP_TIMEOUT} is used.
     */
    @Override
    public void startAsynchronously(ActionListener<Void> listener, @Nullable TimeValue timeout) {
        if (startInitiated.compareAndSet(false, true)) {
            var effectiveTimeout = timeout != null ? timeout : DEFAULT_STARTUP_TIMEOUT;
            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> startInternal(effectiveTimeout));
        }
        // All callers — first and concurrent — register here. SubscribableListener fires immediately
        // if startup is already done, otherwise queues the listener until startInternal completes.
        startupNotifier.addListener(ContextPreservingActionListener.wrapPreservingContext(listener, threadPool.getThreadContext()));
    }

    private void startInternal(TimeValue timeout) {
        try {
            // The manager must be started before the executor service. That way we guarantee that the http client
            // is ready prior to the service attempting to use the http client to send a request
            manager.start();
            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(service::start);
            waitForStartToComplete(timeout);
            startCompleted.set(true);
            startupNotifier.onResponse(null);
        } catch (Exception ex) {
            startupNotifier.onFailure(ex);
        }
    }

    /**
     * Start various internal services. This is required before sending requests.
     *
     * NOTE: This method blocks until the startup is complete.
     */
    @Override
    public void startSynchronously() {
        startSynchronously(DEFAULT_STARTUP_TIMEOUT);
    }

    // Default for testing
    void startSynchronously(TimeValue timeout) {
        if (startInitiated.compareAndSet(false, true)) {
            startInternal(timeout);
        }
        // Handle the case where start*() was already called by another thread and startup is still in progress
        if (startCompleted.get() == false) {
            waitForStartToComplete(timeout);
        }
    }

    private void waitForStartToComplete(TimeValue timeout) {
        try {
            if (startCompletedLatch.await(timeout.getMillis(), TimeUnit.MILLISECONDS) == false) {
                throw new ElasticsearchStatusException("Http sender startup did not complete in time", RestStatus.SERVICE_UNAVAILABLE);
            }
        } catch (InterruptedException e) {
            throw new ElasticsearchStatusException(
                "Http sender interrupted while waiting for startup to complete",
                RestStatus.SERVICE_UNAVAILABLE
            );
        }
    }

    @Override
    public void close() throws IOException {
        manager.close();
        service.shutdown();
    }

    /**
     * Send a request at some point in the future. The timeout used is retrieved from the settings.
     * Startup is initiated automatically if it has not already been started.
     *
     * @param requestCreator  a factory for creating a request to be sent to a 3rd party service
     * @param inferenceInputs the list of string input to send in the request
     * @param timeout         the maximum time the request should wait for a response before timing out. If null, the timeout is ignored.
     *                        The queuing logic may still throw a timeout if it fails to send the request because it couldn't get a leased
     * @param listener        a listener to handle the response
     */
    @Override
    public void send(
        RequestManager requestCreator,
        InferenceInputs inferenceInputs,
        @Nullable TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        SubscribableListener.<Void>newForked(l -> startAsynchronously(l, timeout))
            .<InferenceServiceResults>andThen(sendListener -> service.execute(requestCreator, inferenceInputs, timeout, sendListener))
            .addListener(listener);
    }

    /**
     * This method sends a request and parses the response. It does not leverage any queuing or
     * rate limiting logic. This method should only be used for requests that are not sent often.
     * Startup is initiated automatically if it has not already been started.
     *
     * @param logger          A logger to use for messages
     * @param request         A request to be sent
     * @param responseHandler A handler for parsing the response
     * @param timeout         the maximum time the request should wait for a response before timing out. If null, the timeout is ignored
     * @param listener        a listener to handle the response
     */
    public void sendWithoutQueuing(
        Logger logger,
        Request request,
        ResponseHandler responseHandler,
        @Nullable TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        SubscribableListener.<Void>newForked(l -> startAsynchronously(l, timeout))
            .<InferenceServiceResults>andThen(sendListener -> {
                var preservedListener = ContextPreservingActionListener.wrapPreservingContext(sendListener, threadPool.getThreadContext());
                var timedListener = new TimedListener<>(timeout, preservedListener, threadPool, request.getInferenceEntityId());
                threadPool.executor(UTILITY_THREAD_POOL_NAME)
                    .execute(
                        () -> requestSender.send(logger, request, timedListener::hasCompleted, responseHandler, timedListener.getListener())
                    );
            })
            .addListener(listener);
    }
}
