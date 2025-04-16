/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
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
        private final ServiceComponents serviceComponents;
        private final HttpClientManager httpClientManager;
        private final ClusterService clusterService;
        private final RequestSender requestSender;

        public Factory(ServiceComponents serviceComponents, HttpClientManager httpClientManager, ClusterService clusterService) {
            this.serviceComponents = Objects.requireNonNull(serviceComponents);
            this.httpClientManager = Objects.requireNonNull(httpClientManager);
            this.clusterService = Objects.requireNonNull(clusterService);

            requestSender = new RetryingHttpSender(
                this.httpClientManager.getHttpClient(),
                serviceComponents.throttlerManager(),
                new RetrySettings(serviceComponents.settings(), clusterService),
                serviceComponents.threadPool()
            );
        }

        public Sender createSender() {
            return new HttpRequestSender(
                serviceComponents.threadPool(),
                httpClientManager,
                clusterService,
                serviceComponents.settings(),
                requestSender
            );
        }
    }

    private static final TimeValue START_COMPLETED_WAIT_TIME = TimeValue.timeValueSeconds(5);

    private final ThreadPool threadPool;
    private final HttpClientManager manager;
    private final RequestExecutor service;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final CountDownLatch startCompleted = new CountDownLatch(1);
    private final RequestSender requestSender;

    private HttpRequestSender(
        ThreadPool threadPool,
        HttpClientManager httpClientManager,
        ClusterService clusterService,
        Settings settings,
        RequestSender requestSender
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.manager = Objects.requireNonNull(httpClientManager);
        this.requestSender = Objects.requireNonNull(requestSender);
        service = new RequestExecutorService(
            threadPool,
            startCompleted,
            new RequestExecutorServiceSettings(settings, clusterService),
            requestSender
        );
    }

    /**
     * Start various internal services. This is required before sending requests.
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            // The manager must be started before the executor service. That way we guarantee that the http client
            // is ready prior to the service attempting to use the http client to send a request
            manager.start();
            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(service::start);
            waitForStartToComplete();
        }
    }

    public void updateRateLimitDivisor(int rateLimitDivisor) {
        service.updateRateLimitDivisor(rateLimitDivisor);
    }

    private void waitForStartToComplete() {
        try {
            if (startCompleted.await(START_COMPLETED_WAIT_TIME.getSeconds(), TimeUnit.SECONDS) == false) {
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
        assert started.get() : "call start() before sending a request";
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
        assert started.get() : "call start() before sending a request";
        waitForStartToComplete();

        var preservedListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadPool.getThreadContext());
        var timedListener = new TimedListener<>(timeout, preservedListener, threadPool);

        threadPool.executor(UTILITY_THREAD_POOL_NAME)
            .execute(() -> requestSender.send(logger, request, timedListener::hasCompleted, responseHandler, timedListener.getListener()));
    }
}
