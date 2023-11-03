/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;

public class RetryingHttpSender implements Retrier {
    private static final Logger classLogger = LogManager.getLogger(RetryingHttpSender.class);

    private final Sender sender;
    private final ThrottlerManager throttlerManager;
    private final Logger logger;
    private final RetrySettings retrySettings;

    public RetryingHttpSender(Sender sender, ThrottlerManager throttlerManager, Logger logger, RetrySettings retrySettings) {
        this.sender = Objects.requireNonNull(sender);
        this.throttlerManager = Objects.requireNonNull(throttlerManager);
        this.logger = Objects.requireNonNull(logger);
        this.retrySettings = Objects.requireNonNull(retrySettings);
    }

    @Override
    public void send(HttpRequestBase request, ResponseHandler responseHandler, ActionListener<InferenceResults> listener) {
        int failureCount = 0;
        retryingSend(request, responseHandler, failureCount, listener);
    }

    private void retryingSend(
        HttpRequestBase request,
        ResponseHandler responseHandler,
        int failureCount,
        ActionListener<InferenceResults> listener
    ) {
        ActionListener<HttpResult> responseListener = ActionListener.wrap(result -> {
            try {
                responseHandler.validateResponse(throttlerManager, logger, request, result);
                InferenceResults inferenceResults = responseHandler.parseResult(result);

                listener.onResponse(inferenceResults);
            } catch (ElasticsearchException e) {
                logException(request, result, responseHandler.getRequestType(), e);
                attemptRetry(request, responseHandler, e, failureCount, listener);
            } catch (Exception e) {
                logException(request, result, responseHandler.getRequestType(), e);

                attemptRetry(
                    request,
                    responseHandler,
                    new ElasticsearchException(format("Failed to process request [%s]", request.getRequestLine()), e),
                    failureCount,
                    listener
                );
            }
        }, e -> {
            logException(request, responseHandler.getRequestType(), e);
            attemptRetry(request, responseHandler, e, failureCount, listener);
        });

        sender.send(request, responseListener);
    }

    private void logException(HttpRequestBase request, String requestType, Exception exception) {
        throttlerManager.warn(
            logger,
            format("Failed while sending request [%s] of type [%s]", request.getRequestLine(), requestType),
            exception
        );
    }

    private void logException(HttpRequestBase request, HttpResult result, String requestType, IOException exception) {
        throttlerManager.warn(
            logger,
            format(
                "Failed to parse the response for request [%s] of type [%s] with status [%s] [%s]",
                request.getRequestLine(),
                requestType,
                result.response().getStatusLine().getStatusCode(),
                result.response().getStatusLine().getReasonPhrase()
            ),
            exception
        );
    }

    private void logException(HttpRequestBase request, HttpResult result, String requestType, Exception exception) {
        throttlerManager.warn(
            logger,
            format(
                "Failed to process the response for request [%s] of type [%s] with status [%s] [%s]",
                request.getRequestLine(),
                requestType,
                result.response().getStatusLine().getStatusCode(),
                result.response().getStatusLine().getReasonPhrase()
            ),
            exception
        );
    }

    private void attemptRetry(
        HttpRequestBase request,
        ResponseHandler responseHandler,
        Exception exception,
        int failureCount,
        ActionListener<InferenceResults> listener
    ) {
        try {
            // Let's retry all exceptions for now
            if (failureCount >= retrySettings.getMaxRetries()) {
                listener.onFailure(
                    new ElasticsearchException(
                        format("Failed sending request [%s] after [%s] retries", request.getRequestLine(), retrySettings.getMaxRetries()),
                        exception
                    )
                );
                return;
            }

            classLogger.debug(
                () -> format("Failed sending request [%s], [%s] times, retrying", request.getRequestLine(), failureCount),
                exception
            );

            TimeUnit.SECONDS.sleep(retrySettings.getRetryWaitTime().getSeconds());
            retryingSend(request, responseHandler, failureCount + 1, listener);
        } catch (Exception e) {
            listener.onFailure(
                new ElasticsearchException(
                    format("Failed while attempting to retry request [%s] on retry number [%s]", request.getRequestLine(), failureCount),
                    e
                )
            );
        }
    }
}
