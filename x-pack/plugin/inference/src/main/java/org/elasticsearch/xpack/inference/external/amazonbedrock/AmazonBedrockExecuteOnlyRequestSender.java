/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockChatCompletionRequest;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings.AmazonBedrockEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponseHandler;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

/**
 * The AWS SDK uses its own internal retrier and timeout values on the client
 */
public class AmazonBedrockExecuteOnlyRequestSender implements RequestSender {

    protected final AmazonBedrockClientCache clientCache;
    private final ThrottlerManager throttleManager;

    public AmazonBedrockExecuteOnlyRequestSender(AmazonBedrockClientCache clientCache, ThrottlerManager throttlerManager) {
        this.clientCache = Objects.requireNonNull(clientCache);
        this.throttleManager = Objects.requireNonNull(throttlerManager);
    }

    @Override
    public void send(
        Logger logger,
        Request request,
        Supplier<Boolean> hasRequestTimedOutFunction,
        ResponseHandler responseHandler,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (request instanceof AmazonBedrockRequest awsRequest && responseHandler instanceof AmazonBedrockResponseHandler awsResponse) {
            try {
                var executor = createExecutor(awsRequest, awsResponse, logger, hasRequestTimedOutFunction, listener);

                // the run method will call the listener to return the proper value
                executor.run();
                return;
            } catch (Exception e) {
                logException(logger, request, e);
                listener.onFailure(wrapWithElasticsearchException(e, request.getInferenceEntityId()));
            }
        }

        listener.onFailure(new ElasticsearchException("Amazon Bedrock request was not the correct type"));
    }

    // allow this to be overridden for testing
    protected AmazonBedrockExecutor createExecutor(
        AmazonBedrockRequest awsRequest,
        AmazonBedrockResponseHandler awsResponse,
        Logger logger,
        Supplier<Boolean> hasRequestTimedOutFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        switch (awsRequest.taskType()) {
            case COMPLETION -> {
                return new AmazonBedrockChatCompletionExecutor(
                    (AmazonBedrockChatCompletionRequest) awsRequest,
                    awsResponse,
                    logger,
                    hasRequestTimedOutFunction,
                    listener,
                    clientCache
                );
            }
            case TEXT_EMBEDDING -> {
                return new AmazonBedrockEmbeddingsExecutor(
                    (AmazonBedrockEmbeddingsRequest) awsRequest,
                    awsResponse,
                    logger,
                    hasRequestTimedOutFunction,
                    listener,
                    clientCache
                );
            }
            default -> {
                throw new UnsupportedOperationException("Unsupported task type [" + awsRequest.taskType() + "] for Amazon Bedrock request");
            }
        }
    }

    private void logException(Logger logger, Request request, Exception exception) {
        var causeException = ExceptionsHelper.unwrapCause(exception);

        throttleManager.warn(
            logger,
            format("Failed while sending request from inference entity id [%s] of type [amazonbedrock]", request.getInferenceEntityId()),
            causeException
        );
    }

    private Exception wrapWithElasticsearchException(Exception e, String inferenceEntityId) {
        return new ElasticsearchException(
            format("Amazon Bedrock client failed to send request from inference entity id [%s]", inferenceEntityId),
            e
        );
    }

    public void shutdown() throws IOException {
        this.clientCache.close();
    }
}
