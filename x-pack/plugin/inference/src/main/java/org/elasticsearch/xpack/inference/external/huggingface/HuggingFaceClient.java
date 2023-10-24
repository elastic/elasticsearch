/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.huggingface;

import org.apache.http.ConnectionClosedException;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequest;
import org.elasticsearch.xpack.inference.external.response.huggingface.HuggingFaceElserResponseEntity;

import java.io.IOException;
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

    public void send(HuggingFaceElserRequest request, ActionListener<InferenceResults> listener) throws IOException {
        HttpRequestBase httpRequest = request.createRequest();
        retryingSend(httpRequest, listener);
    }

    private void retryingSend(HttpRequestBase request, ActionListener<InferenceResults> listener) throws IOException {
        AtomicInteger failureCount = new AtomicInteger();

        ActionListener<HttpResult> responseListener = ActionListener.wrap(response -> {
            try {
                listener.onResponse(HuggingFaceElserResponseEntity.fromResponse(response));
            } catch (Exception e) {
                String msg = format("Failed to parse the Hugging Face ELSER response for request [%s]", request.getRequestLine());
                logger.warn(msg, e);
                listener.onFailure(new ElasticsearchException(msg, e));
            }
        }, e -> handleFailure(request, e, failureCount, listener, SEND_REQUEST_RETRY_LIMIT));

        sender.send(request, responseListener);
    }

    private void handleFailure(
        HttpRequestBase request,
        Exception exception,
        AtomicInteger failureCount,
        ActionListener<InferenceResults> listener,
        int retries
    ) {
        try {
            if (exception instanceof ElasticsearchException) {
                listener.onFailure(exception);
                return;
            } else if (exception instanceof ConnectionClosedException == false) {
                listener.onFailure(new ElasticsearchException(exception));
                return;
            }

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

            logger.debug(() -> format("Failed sending request [%s], [%s] times, retrying", request.getRequestLine(), count));
            TimeUnit.SECONDS.sleep(TimeValue.timeValueSeconds(1).getSeconds());
            retryingSend(request, listener);
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
