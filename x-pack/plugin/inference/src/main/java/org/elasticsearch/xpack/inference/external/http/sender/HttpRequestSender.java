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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;
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
        private final SingleRequestManager requestManager;

        public Factory(ServiceComponents serviceComponents, HttpClientManager httpClientManager, ClusterService clusterService) {
            this.serviceComponents = Objects.requireNonNull(serviceComponents);
            this.httpClientManager = Objects.requireNonNull(httpClientManager);
            this.clusterService = Objects.requireNonNull(clusterService);

            var requestSender = new RetryingHttpSender(
                this.httpClientManager.getHttpClient(),
                serviceComponents.throttlerManager(),
                new RetrySettings(serviceComponents.settings(), clusterService),
                serviceComponents.threadPool()
            );
            requestManager = new SingleRequestManager(requestSender);
        }

        public Sender createSender(String serviceName) {
            return new HttpRequestSender(
                serviceName,
                serviceComponents.threadPool(),
                httpClientManager,
                clusterService,
                serviceComponents.settings(),
                requestManager
            );
        }
    }

    private static final Logger logger = LogManager.getLogger(HttpRequestSender.class);
    private static final TimeValue START_COMPLETED_WAIT_TIME = TimeValue.timeValueSeconds(5);

    /**
     * The maximum time a request can take. The timer starts once a request is enqueued and continues until a response is
     * received from the 3rd party service. This encompasses the time the request might just sit in the queue waiting to be sent
     * if another request is already waiting for a connection lease from the connection pool.
     */
    public static final Setting<TimeValue> MAX_REQUEST_TIMEOUT = Setting.timeSetting(
        "xpack.inference.http.max_request_timeout",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final ThreadPool threadPool;
    private final HttpClientManager manager;
    private final RequestExecutorService service;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile TimeValue maxRequestTimeout;
    private final CountDownLatch startCompleted = new CountDownLatch(2);

    private HttpRequestSender(
        String serviceName,
        ThreadPool threadPool,
        HttpClientManager httpClientManager,
        ClusterService clusterService,
        Settings settings,
        SingleRequestManager requestManager
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.manager = Objects.requireNonNull(httpClientManager);
        service = new RequestExecutorService(
            serviceName,
            threadPool,
            startCompleted,
            new RequestExecutorServiceSettings(settings, clusterService),
            requestManager
        );

        this.maxRequestTimeout = MAX_REQUEST_TIMEOUT.get(settings);
        addSettingsUpdateConsumers(clusterService);
    }

    private void addSettingsUpdateConsumers(ClusterService clusterService) {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_REQUEST_TIMEOUT, this::setMaxRequestTimeout);
    }

    // Default for testing
    void setMaxRequestTimeout(TimeValue maxRequestTimeout) {
        logger.debug(() -> format("Max request timeout updated to [%s] for service [%s]", maxRequestTimeout, service));
        this.maxRequestTimeout = maxRequestTimeout;
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
            startCompleted.countDown();
        }
    }

    @Override
    public void close() throws IOException {
        manager.close();
        service.shutdown();
    }

    /**
     * Send a request at some point in the future. The timeout used is retrieved from the settings.
     * @param requestCreator a factory for creating a request to be sent to a 3rd party service
     * @param input the list of string input to send in the request
     * @param timeout the maximum time the request should wait for a response before timing out. If null, the timeout is ignored.
     *                The queuing logic may still throw a timeout if it fails to send the request because it couldn't get a leased
     * @param listener a listener to handle the response
     */
    public void send(
        ExecutableRequestCreator requestCreator,
        List<String> input,
        @Nullable TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        assert started.get() : "call start() before sending a request";
        waitForStartToComplete();
        service.execute(requestCreator, input, timeout, listener);
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

    /**
     * Send a request at some point in the future. The timeout used is retrieved from the settings.
     * @param requestCreator a factory for creating a request to be sent to a 3rd party service
     * @param input the list of string input to send in the request
     * @param listener a listener to handle the response
     */
    public void send(ExecutableRequestCreator requestCreator, List<String> input, ActionListener<InferenceServiceResults> listener) {
        assert started.get() : "call start() before sending a request";
        waitForStartToComplete();
        service.execute(requestCreator, input, maxRequestTimeout, listener);
    }

    public static List<Setting<?>> getSettings() {
        return List.of(MAX_REQUEST_TIMEOUT);
    }
}
