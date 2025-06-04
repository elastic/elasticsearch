/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.SizeLimitInputStream;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

public class RetryingHttpSender implements RequestSender {

    public static final int MAX_RETIES = 3;

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
        private final AtomicInteger retryCount;

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
            this.retryCount = new AtomicInteger(0);
        }

        @Override
        public void tryAction(ActionListener<InferenceServiceResults> listener) {
            retryCount.incrementAndGet();
            // A timeout likely occurred so let's stop attempting to execute the request
            if (hasRequestCompletedFunction.get()) {
                return;
            }

            var retryableListener = listener.delegateResponse((l, e) -> {
                logException(logger, request, responseHandler.getRequestType(), e);
                l.onFailure(transformIfRetryable(e));
            });

            try {
                if (request.isStreaming() && responseHandler.canHandleStreamingResponses()) {
                    httpClient.stream(request.createHttpRequest(), context, retryableListener.delegateFailure((l, r) -> {
                        if (r.isSuccessfulResponse()) {
                            l.onResponse(responseHandler.parseResult(request, r.toHttpResult()));
                        } else {
                            r.readFullResponse(l.delegateFailureAndWrap((ll, httpResult) -> {
                                try {
                                    responseHandler.validateResponse(throttlerManager, logger, request, httpResult, true);
                                    InferenceServiceResults inferenceResults = responseHandler.parseResult(request, httpResult);
                                    ll.onResponse(inferenceResults);
                                } catch (Exception e) {
                                    logException(logger, request, httpResult, responseHandler.getRequestType(), e);
                                    listener.onFailure(e); // skip retrying
                                }
                            }));
                        }
                    }));
                } else {
                    httpClient.send(request.createHttpRequest(), context, retryableListener.delegateFailure((l, r) -> {
                        try {
                            responseHandler.validateResponse(throttlerManager, logger, request, r, false);
                            InferenceServiceResults inferenceResults = responseHandler.parseResult(request, r);

                            l.onResponse(inferenceResults);
                        } catch (Exception e) {
                            logException(logger, request, r, responseHandler.getRequestType(), e);
                            listener.onFailure(e); // skip retrying
                        }
                    }));
                }
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
            } else if (e instanceof SizeLimitInputStream.InputStreamTooLargeException) {
                return e;
            } else if (e instanceof IOException) {
                return new RetryException(true, e);
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
            if (retryCount.get() >= MAX_RETIES) {
                return false;
            }

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
        Supplier<Boolean> hasRequestTimedOutFunction,
        ResponseHandler responseHandler,
        ActionListener<InferenceServiceResults> listener
    ) {
        var retrier = new InternalRetrier(
            logger,
            request,
            HttpClientContext.create(),
            hasRequestTimedOutFunction,
            responseHandler,
            listener
        );
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
