/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
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
        private static final TimeValue START_COMPLETED_WAIT_TIME = TimeValue.timeValueSeconds(5);

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
                startCompleted,
                START_COMPLETED_WAIT_TIME
            );
        }

        public Sender createSender() {
            return httpRequestSender;
        }
    }

    private static final Logger logger = LogManager.getLogger(HttpRequestSender.class);

    private final ThreadPool threadPool;
    private final HttpClientManager manager;
    private final AtomicBoolean startInitiated = new AtomicBoolean(false);
    private final AtomicBoolean startCompleted = new AtomicBoolean(false);
    private final RequestSender requestSender;
    private final RequestExecutor service;
    private final CountDownLatch startCompletedLatch;
    private final TimeValue startCompletedWaitTime;

    // Visible for testing
    protected HttpRequestSender(
        ThreadPool threadPool,
        HttpClientManager httpClientManager,
        RequestSender requestSender,
        RequestExecutor service,
        CountDownLatch startCompletedLatch,
        TimeValue startCompletedWaitTime
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.manager = Objects.requireNonNull(httpClientManager);
        this.requestSender = Objects.requireNonNull(requestSender);
        this.service = Objects.requireNonNull(service);
        this.startCompletedLatch = Objects.requireNonNull(startCompletedLatch);
        this.startCompletedWaitTime = Objects.requireNonNull(startCompletedWaitTime);
    }

    /**
     * Start various internal services asynchronously. This is required before sending requests.
     */
    @Override
    public void startAsynchronously(ActionListener<Void> listener) {
        if (startInitiated.compareAndSet(false, true)) {
            var preservedListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadPool.getThreadContext());
            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> startInternal(preservedListener));
        } else if (startCompleted.get() == false) {
            var preservedListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadPool.getThreadContext());
            // wait on another thread so we don't potential block a transport thread
            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> waitForStartToCompleteWithListener(preservedListener));
        } else {
            listener.onResponse(null);
        }
    }

    private void startInternal(ActionListener<Void> listener) {
        try {
            // The manager must be started before the executor service. That way we guarantee that the http client
            // is ready prior to the service attempting to use the http client to send a request
            manager.start();
            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(service::start);
            waitForStartToComplete();
            startCompleted.set(true);
            listener.onResponse(null);
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    private void waitForStartToCompleteWithListener(ActionListener<Void> listener) {
        try {
            waitForStartToComplete();
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Start various internal services. This is required before sending requests.
     *
     * NOTE: This method blocks until the startup is complete.
     */
    @Override
    public void startSynchronously() {
        if (startInitiated.compareAndSet(false, true)) {
            ActionListener<Void> listener = ActionListener.wrap(
                unused -> {},
                exception -> logger.error("Http sender failed to start", exception)
            );
            startInternal(listener);
        }
        // Handle the case where start*() was already called and this would return immediately because the started flag is already true
        waitForStartToComplete();
    }

    private void waitForStartToComplete() {
        try {
            if (startCompletedLatch.await(startCompletedWaitTime.getMillis(), TimeUnit.MILLISECONDS) == false) {
                throw new IllegalStateException("Http sender startup did not complete in time");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Http sender interrupted while waiting for startup to complete");
        }
    }

    @Override
    public void close() throws IOException {
        manager.close();
        service.shutdown();
    }

    /**
     * Send a request at some point in the future. The timeout used is retrieved from the settings.
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
        assert startInitiated.get() : "call start() before sending a request";
        waitForStartToComplete();
        service.execute(requestCreator, inferenceInputs, timeout, listener);
    }

    /**
     * This method sends a request and parses the response. It does not leverage any queuing or
     * rate limiting logic. This method should only be used for requests that are not sent often.
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
        assert startInitiated.get() : "call start() before sending a request";
        waitForStartToComplete();

        var preservedListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadPool.getThreadContext());
        var timedListener = new TimedListener<>(timeout, preservedListener, threadPool);

        threadPool.executor(UTILITY_THREAD_POOL_NAME)
            .execute(() -> requestSender.send(logger, request, timedListener::hasCompleted, responseHandler, timedListener.getListener()));
    }
}
