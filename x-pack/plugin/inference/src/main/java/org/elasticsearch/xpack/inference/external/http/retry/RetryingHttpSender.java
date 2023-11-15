/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

public class RetryingHttpSender implements Retrier {
    private final Sender sender;
    private final ThrottlerManager throttlerManager;
    private final Logger logger;
    private final RetrySettings retrySettings;
    private final ThreadPool threadPool;
    private final Executor executor;

    public RetryingHttpSender(
        Sender sender,
        ThrottlerManager throttlerManager,
        Logger logger,
        RetrySettings retrySettings,
        ThreadPool threadPool
    ) {
        this(sender, throttlerManager, logger, retrySettings, threadPool, threadPool.executor(UTILITY_THREAD_POOL_NAME));
    }

    // For testing only
    RetryingHttpSender(
        Sender sender,
        ThrottlerManager throttlerManager,
        Logger logger,
        RetrySettings retrySettings,
        ThreadPool threadPool,
        Executor executor
    ) {
        this.sender = Objects.requireNonNull(sender);
        this.throttlerManager = Objects.requireNonNull(throttlerManager);
        this.logger = Objects.requireNonNull(logger);
        this.retrySettings = Objects.requireNonNull(retrySettings);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.executor = Objects.requireNonNull(executor);
    }

    private class InternalRetrier extends RetryableAction<List<? extends InferenceResults>> {
        private final HttpRequestBase request;
        private final ResponseHandler responseHandler;

        InternalRetrier(
            HttpRequestBase request,
            ResponseHandler responseHandler,
            ActionListener<List<? extends InferenceResults>> listener
        ) {
            super(
                logger,
                threadPool,
                retrySettings.getSettings().initialDelay(),
                retrySettings.getSettings().maxDelayBound(),
                retrySettings.getSettings().timeoutValue(),
                listener,
                executor
            );
            this.request = request;
            this.responseHandler = responseHandler;
        }

        @Override
        public void tryAction(ActionListener<List<? extends InferenceResults>> listener) {
            ActionListener<HttpResult> responseListener = ActionListener.wrap(result -> {
                try {
                    responseHandler.validateResponse(throttlerManager, logger, request, result);
                    List<? extends InferenceResults> inferenceResults = responseHandler.parseResult(result);

                    listener.onResponse(inferenceResults);
                } catch (Exception e) {
                    logException(request, result, responseHandler.getRequestType(), e);
                    listener.onFailure(e);
                }
            }, e -> {
                logException(request, responseHandler.getRequestType(), e);
                listener.onFailure(transformIfRetryable(e));
            });

            sender.send(request, responseListener);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            if (e instanceof RetryException) {
                return ((RetryException) e).shouldRetry();
            }

            return false;
        }

        /**
         * If the connection gets closed by the server or because of the connections time to live is exceeded we'll likely get a
         * {@link org.apache.http.ConnectionClosedException} exception which is a child of IOException. For now,
         * we'll consider all IOExceptions retryable because something failed while we were trying to send the request
         * @param e the Exception received while sending the request
         * @return a {@link RetryException} if this exception can be retried
         */
        private Exception transformIfRetryable(Exception e) {
            var exceptionToReturn = e;
            if (e instanceof IOException) {
                exceptionToReturn = new RetryException(true, e);
            }

            return exceptionToReturn;
        }
    }

    @Override
    public void send(HttpRequestBase request, ResponseHandler responseHandler, ActionListener<List<? extends InferenceResults>> listener) {
        InternalRetrier retrier = new InternalRetrier(request, responseHandler, listener);
        retrier.run();
    }

    private void logException(HttpRequestBase request, String requestType, Exception exception) {
        var causeException = ExceptionsHelper.unwrapCause(exception);

        throttlerManager.warn(
            logger,
            format("Failed while sending request [%s] of type [%s]", request.getRequestLine(), requestType),
            causeException
        );
    }

    private void logException(HttpRequestBase request, HttpResult result, String requestType, Exception exception) {
        var causeException = ExceptionsHelper.unwrapCause(exception);

        throttlerManager.warn(
            logger,
            format(
                "Failed to process the response for request [%s] of type [%s] with status [%s] [%s]",
                request.getRequestLine(),
                requestType,
                result.response().getStatusLine().getStatusCode(),
                result.response().getStatusLine().getReasonPhrase()
            ),
            causeException
        );
    }
}
