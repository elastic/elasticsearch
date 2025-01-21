/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.auth;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.elastic.ElasticInferenceServiceResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.elastic.ElasticInferenceServiceAuthRequest;
import org.elasticsearch.xpack.inference.external.response.elastic.ElasticInferenceServiceAuthResponseEntity;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xpack.core.inference.action.InferenceAction.Request.DEFAULT_TIMEOUT;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.ELASTIC_INFERENCE_SERVICE_IDENTIFIER;

/**
 * Handles retrieving the authorization information from the EIS gateway.
 */
public class ElasticInferenceServiceAuthorizationHandler {
    private static final Logger logger = LogManager.getLogger(ElasticInferenceServiceAuthorizationHandler.class);

    private static final ResponseHandler AUTH_RESPONSE_HANDLER = createAuthResponseHandler();

    private static ResponseHandler createAuthResponseHandler() {
        return new ElasticInferenceServiceResponseHandler(
            String.format(Locale.ROOT, "%s sparse embeddings", ELASTIC_INFERENCE_SERVICE_IDENTIFIER),
            ElasticInferenceServiceAuthResponseEntity::fromResponse
        );
    }

    private final String baseUrl;
    private final Sender sender;
    private final ThreadPool threadPool;

    public ElasticInferenceServiceAuthorizationHandler(String baseUrl, Sender sender, ThreadPool threadPool) {
        this.baseUrl = Objects.requireNonNull(baseUrl);
        this.sender = Objects.requireNonNull(sender);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    /**
     * Retrieve the authorization information from EIS
     * @param listener a listener to receive the response
     */
    public void getAuth(ActionListener<ElasticInferenceServiceAuthorization> listener) {
        try {
            // ensure that the sender is initialized
            sender.start();

            ActionListener<InferenceServiceResults> newListener = ActionListener.wrap(results -> {
                if (results instanceof ElasticInferenceServiceAuthResponseEntity authResponseEntity) {
                    listener.onResponse(ElasticInferenceServiceAuthorization.of(authResponseEntity));
                } else {
                    logger.warn(
                        Strings.format(
                            "Failed to retrieve the authorization information from the Elastic Inference Service gateway. "
                                + "Received an invalid response type: %s",
                            results.getClass().getSimpleName()
                        )
                    );
                }
                listener.onResponse(ElasticInferenceServiceAuthorization.newDisabledService());
            }, e -> {
                logger.warn(
                    Strings.format(
                        "Failed to retrieve the authorization information from the "
                            + "Elastic Inference Service gateway. Encountered an exception: %s",
                        e
                    )
                );
                listener.onResponse(ElasticInferenceServiceAuthorization.newDisabledService());
            });

            var request = new ElasticInferenceServiceAuthRequest(baseUrl, getCurrentTraceInfo());

            sender.sendWithoutQueuing(logger, request, AUTH_RESPONSE_HANDLER, DEFAULT_TIMEOUT, newListener);
        } catch (Exception e) {
            logger.warn(
                Strings.format("Failed to retrieve the authorization information from the Elastic Inference Service gateway: %s", e)
            );
        }
    }

    // TODO do we need this?
    private TraceContext getCurrentTraceInfo() {
        var traceParent = threadPool.getThreadContext().getHeader(Task.TRACE_PARENT);
        var traceState = threadPool.getThreadContext().getHeader(Task.TRACE_STATE);

        return new TraceContext(traceParent, traceState);
    }
}
