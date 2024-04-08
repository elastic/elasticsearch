/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

public class RetryingHttpSender implements RequestSender {
    private final HttpClient httpClient;
    private final ThrottlerManager throttlerManager;
    private final RetrySettings retrySettings;
    private final ThreadPool threadPool;
    private final Executor executor;

    public RetryingHttpSender(
        HttpClient httpClient,
        ThrottlerManager throttlerManager,
        RetrySettings retrySettings,
        ThreadPool threadPool
    ) {
        this(httpClient, throttlerManager, retrySettings, threadPool, threadPool.executor(UTILITY_THREAD_POOL_NAME));
    }

    // For testing only
    RetryingHttpSender(
        HttpClient httpClient,
        ThrottlerManager throttlerManager,
        RetrySettings retrySettings,
        ThreadPool threadPool,
        Executor executor
    ) {
        this.httpClient = Objects.requireNonNull(httpClient);
        this.throttlerManager = Objects.requireNonNull(throttlerManager);
        this.retrySettings = Objects.requireNonNull(retrySettings);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.executor = Objects.requireNonNull(executor);
    }

    private class InternalRetrier extends RetryableAction<InferenceServiceResults> {
        private Request request;
        private final ResponseHandler responseHandler;
        private final Logger logger;
        private final HttpClientContext context;
        private final Supplier<Boolean> hasRequestCompletedFunction;

        InternalRetrier(
            Logger logger,
            Request request,
            HttpClientContext context,
            Supplier<Boolean> hasRequestCompletedFunction,
            ResponseHandler responseHandler,
            ActionListener<InferenceServiceResults> listener
        ) {
            super(
                Objects.requireNonNull(logger),
                threadPool,
                retrySettings.getInitialDelay(),
                retrySettings.getMaxDelayBound(),
                retrySettings.getTimeout(),
                listener,
                executor
            );
            this.logger = logger;
            this.request = Objects.requireNonNull(request);
            this.context = Objects.requireNonNull(context);
            this.responseHandler = Objects.requireNonNull(responseHandler);
            this.hasRequestCompletedFunction = Objects.requireNonNull(hasRequestCompletedFunction);
        }

        @Override
        public void tryAction(ActionListener<InferenceServiceResults> listener) {
            // A timeout likely occurred so let's stop attempting to execute the request
            if (hasRequestCompletedFunction.get()) {
                return;
            }

            ActionListener<HttpResult> responseListener = ActionListener.wrap(result -> {
                try {
                    responseHandler.validateResponse(throttlerManager, logger, request, result);
                    InferenceServiceResults inferenceResults = responseHandler.parseResult(request, result);

                    listener.onResponse(inferenceResults);
                } catch (Exception e) {
                    logException(logger, request, result, responseHandler.getRequestType(), e);
                    listener.onFailure(e);
                }
            }, e -> {
                logException(logger, request, responseHandler.getRequestType(), e);
                listener.onFailure(transformIfRetryable(e));
            });

            try {
                httpClient.send(request.createHttpRequest(), context, responseListener);
            } catch (Exception e) {
                logException(logger, request, responseHandler.getRequestType(), e);

                listener.onFailure(wrapWithElasticsearchException(e, request.getInferenceEntityId()));
            }
        }

        /**
         * If the connection gets closed by the server or because of the connections time to live is exceeded we'll likely get a
         * {@link org.apache.http.ConnectionClosedException} exception which is a child of IOException.
         *
         * @param e the Exception received while sending the request
         * @return a {@link RetryException} if this exception can be retried
         */
        private Exception transformIfRetryable(Exception e) {
            var exceptionToReturn = e;

            if (e instanceof UnknownHostException) {
                return new ElasticsearchStatusException(
                    format("Invalid host [%s], please check that the URL is correct.", request.getURI()),
                    RestStatus.BAD_REQUEST,
                    e
                );
            }

            if (e instanceof IOException) {
                exceptionToReturn = new RetryException(true, e);
            }

            return exceptionToReturn;
        }

        private Exception wrapWithElasticsearchException(Exception e, String inferenceEntityId) {
            var transformedException = transformIfRetryable(e);

            if (transformedException instanceof ElasticsearchException) {
                return transformedException;
            }

            return new ElasticsearchException(
                format("Http client failed to send request from inference entity id [%s]", inferenceEntityId),
                transformedException
            );
        }

        @Override
        public boolean shouldRetry(Exception e) {
            if (e instanceof Retryable retry) {
                request = retry.rebuildRequest(request);
                return retry.shouldRetry();
            }

            return false;
        }
    }

    @Override
    public void send(
        Logger logger,
        Request request,
        HttpClientContext context,
        Supplier<Boolean> hasRequestTimedOutFunction,
        ResponseHandler responseHandler,
        ActionListener<InferenceServiceResults> listener
    ) {
        InternalRetrier retrier = new InternalRetrier(logger, request, context, hasRequestTimedOutFunction, responseHandler, listener);
        retrier.run();
    }

    private void logException(Logger logger, Request request, String requestType, Exception exception) {
        var causeException = ExceptionsHelper.unwrapCause(exception);

        throttlerManager.warn(
            logger,
            format("Failed while sending request from inference entity id [%s] of type [%s]", request.getInferenceEntityId(), requestType),
            causeException
        );
    }

    private void logException(Logger logger, Request request, HttpResult result, String requestType, Exception exception) {
        var causeException = ExceptionsHelper.unwrapCause(exception);

        throttlerManager.warn(
            logger,
            format(
                "Failed to process the response for request from inference entity id [%s] of type [%s] with status [%s] [%s]",
                request.getInferenceEntityId(),
                requestType,
                result.response().getStatusLine().getStatusCode(),
                result.response().getStatusLine().getReasonPhrase()
            ),
            causeException
        );
    }
}
