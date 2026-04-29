/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ListenerTimeouts;
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

            var executorServiceSettings = new RequestExecutorServiceSettings(serviceComponents.settings());
            executorServiceSettings.init(clusterService);

            var service = new RequestExecutorService(serviceComponents.threadPool(), executorServiceSettings, requestSender);

            httpRequestSender = new HttpRequestSender(serviceComponents.threadPool(), httpClientManager, requestSender, service);
        }

        public Sender createSender() {
            return httpRequestSender;
        }
    }

    private static final Logger logger = LogManager.getLogger(HttpRequestSender.class);
    private static final TimeValue STARTUP_TIMEOUT = TimeValue.timeValueSeconds(5);

    private final ThreadPool threadPool;
    private final HttpClientManager manager;
    private final AtomicBoolean startInitiated = new AtomicBoolean(false);
    private final RequestSender requestSender;
    private final RequestExecutor service;
    private final SubscribableListener<Void> startupNotifier = new SubscribableListener<>();

    // Visible for testing
    protected HttpRequestSender(
        ThreadPool threadPool,
        HttpClientManager httpClientManager,
        RequestSender requestSender,
        RequestExecutor service
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.manager = Objects.requireNonNull(httpClientManager);
        this.requestSender = Objects.requireNonNull(requestSender);
        this.service = Objects.requireNonNull(service);
    }

    /**
     * This should only be called internally to the class or directly by tests.
     * Start various internal services asynchronously. This is required before sending requests.
     * All callers (including concurrent ones) are notified via {@code listener} when startup completes
     * or fails, without blocking any thread pool threads while waiting.
     * <p>
     * If {@code timeout} is non-null, the caller's listener is individually wrapped with that timeout.
     * A per-caller timeout never poisons the shared startup state — subsequent callers can still
     * succeed once startup completes. Only a real failure (e.g. {@code manager.start()} or
     * {@code service.start()} throwing) permanently fails all pending listeners.
     *
     * @param timeout if non-null, the maximum time this specific caller will wait for startup;
     *                other callers are unaffected if this caller's timeout fires.
     */
    void startAsynchronously(ActionListener<Void> listener, @Nullable TimeValue timeout) {
        if (startInitiated.compareAndSet(false, true)) {
            try {
                threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(this::startInternal);
            } catch (Exception e) {
                // Consider making this an error log because it's not recoverable
                logger.warn("Failed to execute http sender start thread", e);
                startupNotifier.onFailure(
                    new ElasticsearchStatusException(
                        "Failed to begin initializing inference components",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        e
                    )
                );
            }
        }
        // Preserve context before wrapping with timeout so both the normal completion path and
        // the timeout action restore the original thread context when notifying the caller.
        var contextPreservedListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadPool.getThreadContext());
        var wrappedListener = timeout == null
            ? contextPreservedListener
            : ListenerTimeouts.wrapWithTimeout(
                threadPool,
                timeout,
                threadPool.executor(UTILITY_THREAD_POOL_NAME),
                contextPreservedListener,
                ignored -> contextPreservedListener.onFailure(
                    new ElasticsearchStatusException("Http sender startup did not complete in time", RestStatus.SERVICE_UNAVAILABLE)
                )
            );
        // All callers — first and concurrent — register here. SubscribableListener fires immediately
        // if startup is already done, otherwise queues the listener until startInternal completes.
        startupNotifier.addListener(wrappedListener);
    }

    private void startInternal() {
        try {
            // The manager must be started before the executor service. That way we guarantee that the http client
            // is ready prior to the service attempting to use the http client to send a request
            manager.start();
            service.start();
            startupNotifier.onResponse(null);
        } catch (Exception e) {
            // Consider making this an error log because it's not recoverable
            logger.warn("Failed to initialize http sender components", e);
            startupNotifier.onFailure(
                new ElasticsearchStatusException("Failed to initialize inference components", RestStatus.INTERNAL_SERVER_ERROR, e)
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
        SubscribableListener.<Void>newForked(startListener -> startAsynchronously(startListener, STARTUP_TIMEOUT))
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
        SubscribableListener.<Void>newForked(startListener -> startAsynchronously(startListener, STARTUP_TIMEOUT))
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
