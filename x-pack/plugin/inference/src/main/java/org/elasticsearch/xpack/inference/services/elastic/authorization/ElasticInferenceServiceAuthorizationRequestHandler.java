/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchWrapperException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceResponseHandler;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceAuthorizationRequest;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.ELASTIC_INFERENCE_SERVICE_IDENTIFIER;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.extractRequestMetadataFromThreadContext;

/**
 * Handles retrieving the authorization information from Elastic Inference Service.
 */
public class ElasticInferenceServiceAuthorizationRequestHandler {

    private static final String FAILED_TO_RETRIEVE_MESSAGE =
        "Failed to retrieve the authorization information from the Elastic Inference Service.";
    private static final TimeValue DEFAULT_AUTH_TIMEOUT = TimeValue.timeValueMinutes(1);
    private static final ResponseHandler AUTH_RESPONSE_HANDLER = createAuthResponseHandler();

    private static ResponseHandler createAuthResponseHandler() {
        return new ElasticInferenceServiceResponseHandler(
            Strings.format("%s authorization", ELASTIC_INFERENCE_SERVICE_IDENTIFIER),
            ElasticInferenceServiceAuthorizationResponseEntity::fromResponse
        );
    }

    private final String baseUrl;
    private final ThreadPool threadPool;
    private final Logger logger;
    private final CountDownLatch requestCompleteLatch = new CountDownLatch(1);

    public ElasticInferenceServiceAuthorizationRequestHandler(@Nullable String baseUrl, ThreadPool threadPool) {
        this.baseUrl = baseUrl;
        this.threadPool = Objects.requireNonNull(threadPool);
        logger = LogManager.getLogger(ElasticInferenceServiceAuthorizationRequestHandler.class);
    }

    // only use for testing
    ElasticInferenceServiceAuthorizationRequestHandler(@Nullable String baseUrl, ThreadPool threadPool, Logger logger) {
        this.baseUrl = baseUrl;
        this.threadPool = Objects.requireNonNull(threadPool);
        this.logger = Objects.requireNonNull(logger);
    }

    /**
     * Retrieve the authorization information from Elastic Inference Service
     * @param listener a listener to receive the response
     * @param sender a {@link Sender} for making the request to the Elastic Inference Service
     */
    public void getAuthorization(ActionListener<ElasticInferenceServiceAuthorizationModel> listener, Sender sender) {
        try {
            logger.debug("Retrieving authorization information from the Elastic Inference Service.");

            if (Strings.isNullOrEmpty(baseUrl)) {
                logger.debug("The base URL for the authorization service is not valid, rejecting authorization.");
                listener.onResponse(ElasticInferenceServiceAuthorizationModel.newDisabledService());
                return;
            }

            // ensure that the sender is initialized
            sender.start();

            ActionListener<InferenceServiceResults> newListener = ActionListener.wrap(results -> {
                if (results instanceof ElasticInferenceServiceAuthorizationResponseEntity authResponseEntity) {
                    listener.onResponse(ElasticInferenceServiceAuthorizationModel.of(authResponseEntity));
                } else {
                    logger.warn(
                        Strings.format(
                            FAILED_TO_RETRIEVE_MESSAGE + " Received an invalid response type: %s",
                            results.getClass().getSimpleName()
                        )
                    );
                    listener.onResponse(ElasticInferenceServiceAuthorizationModel.newDisabledService());
                }
                requestCompleteLatch.countDown();
            }, e -> {
                Throwable exception = e;
                if (e instanceof ElasticsearchWrapperException wrapperException) {
                    exception = wrapperException.getCause();
                }

                logger.warn(Strings.format(FAILED_TO_RETRIEVE_MESSAGE + " Encountered an exception: %s", exception));
                listener.onResponse(ElasticInferenceServiceAuthorizationModel.newDisabledService());
                requestCompleteLatch.countDown();
            });

            var requestMetadata = extractRequestMetadataFromThreadContext(threadPool.getThreadContext());
            var request = new ElasticInferenceServiceAuthorizationRequest(baseUrl, getCurrentTraceInfo(), requestMetadata);

            sender.sendWithoutQueuing(logger, request, AUTH_RESPONSE_HANDLER, DEFAULT_AUTH_TIMEOUT, newListener);
        } catch (Exception e) {
            logger.warn(Strings.format("Retrieving the authorization information encountered an exception: %s", e));
            requestCompleteLatch.countDown();
        }
    }

    private TraceContext getCurrentTraceInfo() {
        var traceParent = threadPool.getThreadContext().getHeader(Task.TRACE_PARENT);
        var traceState = threadPool.getThreadContext().getHeader(Task.TRACE_STATE);

        return new TraceContext(traceParent, traceState);
    }

    // Default because should only be used for testing
    void waitForAuthRequestCompletion(TimeValue timeValue) throws IllegalStateException {
        try {
            if (requestCompleteLatch.await(timeValue.getMillis(), TimeUnit.MILLISECONDS) == false) {
                throw new IllegalStateException("The wait time has expired for authorization to complete.");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Waiting for authorization to complete was interrupted");
        }
    }
}
