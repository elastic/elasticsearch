/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.huggingface;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequest;
import org.elasticsearch.xpack.inference.external.response.huggingface.HuggingFaceElserResponseEntity;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.core.Strings.format;

public class HuggingFaceClient {
    private static final Logger logger = LogManager.getLogger(HuggingFaceClient.class);
    private static final int SEND_REQUEST_RETRY_LIMIT = 5;

    private final Sender sender;

    public HuggingFaceClient(Sender sender) {
        this.sender = sender;
    }

    public void send(HuggingFaceElserRequest request, ActionListener<InferenceResults> listener) {
        HttpRequestBase httpRequest = request.createRequest();
        AtomicInteger failureCount = new AtomicInteger();
        retryingSend(httpRequest, failureCount, listener);
    }

    private void retryingSend(HttpRequestBase request, AtomicInteger failureCount, ActionListener<InferenceResults> listener) {
        ActionListener<HttpResult> responseListener = ActionListener.wrap(result -> {
            try {
                checkForFailureStatusCode(request, result);
                checkForEmptyBody(request, result);

                TextExpansionResults textExpansionResults = HuggingFaceElserResponseEntity.fromResponse(result);
                listener.onResponse(textExpansionResults);
            } catch (ElasticsearchException e) {
                logException(request, result, e);
                attemptRetry(request, e, failureCount, listener, SEND_REQUEST_RETRY_LIMIT);
            } catch (Exception e) {
                logException(request, result, e);

                attemptRetry(
                    request,
                    new ElasticsearchException(
                        format("Failed to process the Hugging Face ELSER response for request [%s]", request.getRequestLine()),
                        e
                    ),
                    failureCount,
                    listener,
                    SEND_REQUEST_RETRY_LIMIT
                );
            }
        }, e -> attemptRetry(request, e, failureCount, listener, SEND_REQUEST_RETRY_LIMIT));

        sender.send(request, responseListener);
    }

    private static void checkForFailureStatusCode(HttpRequestBase request, HttpResult result) {
        if (result.response().getStatusLine().getStatusCode() >= 300) {
            logger.warn(
                format(
                    "Received a failure status code for request [%s] status [%s] body [%s]",
                    request.getRequestLine(),
                    result.response().getStatusLine().getStatusCode(),
                    // TODO is it PII issue or other potential issue if we log the response body?
                    new String(result.body(), StandardCharsets.UTF_8)
                )
            );

            throw new IllegalStateException(
                format(
                    "Received a failure status code for request [%s] status [%s]",
                    request.getRequestLine(),
                    result.response().getStatusLine().getStatusCode()
                )
            );
        }
    }

    private static void checkForEmptyBody(HttpRequestBase request, HttpResult result) {
        if (result.isBodyEmpty()) {
            logger.warn(format("Response body was empty for request [%s]", request.getRequestLine()));
            throw new IllegalStateException(format("Response body was empty for request [%s]", request.getRequestLine()));
        }
    }

    private static void logException(HttpRequestBase request, HttpResult result, Exception exception) {
        logger.warn(
            format(
                "Failed to process the Hugging Face ELSER response for request [%s] status [%s] [%s]",
                request.getRequestLine(),
                result.response().getStatusLine().getStatusCode(),
                result.response().getStatusLine().getReasonPhrase()
            ),
            exception
        );
    }

    private void attemptRetry(
        HttpRequestBase request,
        Exception exception,
        AtomicInteger failureCount,
        ActionListener<InferenceResults> listener,
        int retries
    ) {
        try {
            // Let's retry all exceptions for now
            var count = failureCount.incrementAndGet();
            if (count >= retries) {
                listener.onFailure(
                    new ElasticsearchException(
                        format("Failed sending request [%s] after [%s] retries", request.getRequestLine(), retries),
                        exception
                    )
                );
                return;
            }

            logger.debug(() -> format("Failed sending request [%s], [%s] times, retrying", request.getRequestLine(), count), exception);
            TimeUnit.SECONDS.sleep(TimeValue.timeValueSeconds(1).getSeconds());
            retryingSend(request, failureCount, listener);
        } catch (Exception e) {
            listener.onFailure(
                new ElasticsearchException(
                    format("Failed while attempting to retry request [%s] on retry [%s]", request.getRequestLine(), failureCount.get()),
                    e
                )
            );
        }
    }
}
