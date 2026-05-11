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
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.SizeLimitInputStream;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
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

    public static final int MAX_RETRIES = 3;

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

    private static class SenderException extends RuntimeException {
        private final HttpResult result;

        SenderException(HttpResult result, Exception e) {
            super(e);
            this.result = result;
        }

        public Exception getOriginalException() {
            var cause = getCause();
            if (cause instanceof Exception e) {
                return e;
            } else {
                return new ElasticsearchException(cause);
            }
        }

        public HttpResult getResult() {
            return result;
        }
    }

    private class InternalRetrier extends RetryableAction<InferenceServiceResults> {
        private OutboundRequest outboundRequest;
        private final ResponseHandler responseHandler;
        private final Logger logger;
        private final HttpClientContext context;
        private final Supplier<Boolean> hasRequestCompletedFunction;
        private final AtomicInteger retryCount;

        InternalRetrier(
            Logger logger,
            OutboundRequest outboundRequest,
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
            this.outboundRequest = Objects.requireNonNull(outboundRequest);
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
                // TimedListener will drop this, just being safe to avoid a hanging listener
                listener.onFailure(
                    new ElasticsearchStatusException(timeoutString(outboundRequest.getInferenceEntityId()), RestStatus.GATEWAY_TIMEOUT)
                );
                return;
            }

            var failureListener = listener.delegateResponse((l, e) -> {
                var exceptionToUse = e;

                if (exceptionToUse instanceof SenderException senderException) {
                    exceptionToUse = senderException.getOriginalException();

                    logResponseException(
                        logger,
                        outboundRequest,
                        senderException.getResult(),
                        responseHandler.getRequestType(),
                        exceptionToUse
                    );
                } else {
                    logRequestException(logger, outboundRequest, responseHandler.getRequestType(), exceptionToUse);
                }

                /*
                 * We will try to determine if the exception is retryable and if so wrap it in a RetryException
                 * so that when we pass the failure to the tryAction original listener it will get passed to shouldRetry() and be retried.
                 * If it is already a RetryException, then transformIfRetryable will return it as is and
                 * it will be retried again until we hit the max retry attempts.
                 */
                l.onFailure(transformIfRetryable(exceptionToUse));
            });

            SubscribableListener.<HttpRequest>newForked(
                createHttpRequestListener -> outboundRequest.createHttpRequest(createHttpRequestListener)
            ).<InferenceServiceResults>andThen((inferenceServiceResultsActionListener, httpRequest) -> {
                if (hasRequestCompletedFunction.get()) {
                    // TimedListener will drop this, just being safe to avoid a hanging listener
                    inferenceServiceResultsActionListener.onFailure(
                        new ElasticsearchStatusException(timeoutString(outboundRequest.getInferenceEntityId()), RestStatus.GATEWAY_TIMEOUT)
                    );
                    return;
                }

                sendRequest(httpRequest, inferenceServiceResultsActionListener);
            }).addListener(failureListener);
        }

        private static String timeoutString(String inferenceId) {
            return Strings.format("Inference endpoint [%s]: request timed out", inferenceId);
        }

        private void sendRequest(HttpRequest httpRequest, ActionListener<InferenceServiceResults> listener) throws IOException {
            if (outboundRequest.isStreaming() && responseHandler.canHandleStreamingResponses()) {
                httpClient.stream(httpRequest, context, listener.delegateFailureAndWrap((l, r) -> {
                    if (r.isSuccessfulResponse()) {
                        l.onResponse(responseHandler.parseResult(outboundRequest, r.toHttpResult()));
                    } else {
                        r.readFullResponse(
                            l.delegateFailureAndWrap(
                                (delegateListener, httpResult) -> validateAndParseInferenceResults(httpResult, delegateListener)
                            )
                        );
                    }
                }));
            } else {
                httpClient.send(
                    httpRequest,
                    context,
                    listener.delegateFailureAndWrap(
                        (delegateListener, httpResult) -> validateAndParseInferenceResults(httpResult, delegateListener)
                    )
                );
            }
        }

        private void validateAndParseInferenceResults(HttpResult httpResult, ActionListener<InferenceServiceResults> listener) {
            try {
                responseHandler.validateResponse(throttlerManager, logger, outboundRequest, httpResult);
                InferenceServiceResults inferenceResults = responseHandler.parseResult(outboundRequest, httpResult);

                listener.onResponse(inferenceResults);
            } catch (Exception e) {
                listener.onFailure(new SenderException(httpResult, e));
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
            if (e instanceof UnknownHostException) {
                return new ElasticsearchStatusException(
                    format("Invalid host [%s], please check that the URL is correct.", outboundRequest.getURI()),
                    RestStatus.BAD_REQUEST,
                    e
                );
            } else if (e instanceof SizeLimitInputStream.InputStreamTooLargeException) {
                return e;
            } else if (e instanceof IOException) {
                return new RetryException(true, e);
            }

            return e;
        }

        @Override
        public boolean shouldRetry(Exception e) {
            if (retryCount.get() >= MAX_RETRIES) {
                return false;
            }

            if (e instanceof Retryable retry) {
                outboundRequest = retry.rebuildRequest(outboundRequest);
                return retry.shouldRetry();
            }

            return false;
        }
    }

    @Override
    public void send(
        Logger logger,
        OutboundRequest outboundRequest,
        Supplier<Boolean> hasRequestTimedOutFunction,
        ResponseHandler responseHandler,
        ActionListener<InferenceServiceResults> listener
    ) {
        var retrier = new InternalRetrier(
            logger,
            outboundRequest,
            HttpClientContext.create(),
            hasRequestTimedOutFunction,
            responseHandler,
            listener
        );
        retrier.run();
    }

    private void logResponseException(
        Logger logger,
        OutboundRequest outboundRequest,
        @Nullable HttpResult result,
        String requestType,
        Exception exception
    ) {
        if (result == null) {
            logRequestException(logger, outboundRequest, requestType, exception);
            return;
        }

        var causeException = ExceptionsHelper.unwrapCause(exception);

        throttlerManager.warn(
            logger,
            format(
                "Failed to process the response for request from inference entity id [%s] of type [%s] with status [%s] [%s]",
                outboundRequest.getInferenceEntityId(),
                requestType,
                result.response().getStatusLine().getStatusCode(),
                result.response().getStatusLine().getReasonPhrase()
            ),
            causeException
        );
    }

    private void logRequestException(Logger logger, OutboundRequest outboundRequest, String requestType, Exception exception) {
        var causeException = ExceptionsHelper.unwrapCause(exception);

        throttlerManager.warn(
            logger,
            format(
                "Failed while sending request from inference entity id [%s] of type [%s]",
                outboundRequest.getInferenceEntityId(),
                requestType
            ),
            causeException
        );
    }
}
