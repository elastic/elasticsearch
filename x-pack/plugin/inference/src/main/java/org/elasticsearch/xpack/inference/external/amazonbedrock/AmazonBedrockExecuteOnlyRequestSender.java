/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponse;

import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

/**
 * The AWS SDK uses its own internal retrier and timeout values
 */
public class AmazonBedrockExecuteOnlyRequestSender implements RequestSender {

    public AmazonBedrockExecuteOnlyRequestSender() {}

    @Override
    public void send(
        Logger logger,
        Request request,
        HttpClientContext context,
        Supplier<Boolean> hasRequestTimedOutFunction,
        ResponseHandler responseHandler,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (request instanceof AmazonBedrockRequest awsRequest && responseHandler instanceof AmazonBedrockResponse awsResponse) {
            try {
                var executor = new AmazonBedrockExecutor(
                    awsRequest.model(),
                    awsRequest,
                    awsResponse,
                    logger,
                    hasRequestTimedOutFunction,
                    listener
                );

                // the run method will call the listener to return the proper value
                executor.run();
            } catch (Exception e) {
                // TODO - change from static AmazonBedrock to request type specified
                logException(logger, request, "Amazon Bedrock", e);
                listener.onFailure(wrapWithElasticsearchException(e, request.getInferenceEntityId()));
            }
        }

    }

    private void logException(Logger logger, Request request, String requestType, Exception exception) {
        var causeException = ExceptionsHelper.unwrapCause(exception);

        logger.warn(
            format("Failed while sending request from inference entity id [%s] of type [%s]", request.getInferenceEntityId(), requestType),
            causeException
        );
    }

    private Exception wrapWithElasticsearchException(Exception e, String inferenceEntityId) {
        return new ElasticsearchException(
            format("Amazon Bedrock client failed to send request from inference entity id [%s]", inferenceEntityId),
            e
        );
    }
}
