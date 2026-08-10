/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.response;

import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

public abstract class AmazonBedrockResponseHandler implements ResponseHandler {

    @Override
    public boolean canHandleStreamingResponses() {
        return false;
    }

    @Override
    public final void validateResponse(ThrottlerManager throttlerManager, Logger logger, OutboundRequest outboundRequest, HttpResult result)
        throws RetryException {
        // do nothing as the AWS SDK will take care of validation for us
    }

    /**
     * Bedrock responses are validated by the AWS SDK before they reach this handler, and Bedrock requests never flow through
     * {@link org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender}, so there is no failure status code to
     * translate here.
     */
    @Override
    public final RetryException buildFailureStatusCodeException(OutboundRequest outboundRequest, HttpResult result) {
        assert false : "Amazon Bedrock responses are validated by the AWS SDK";
        throw new UnsupportedOperationException("Amazon Bedrock responses are validated by the AWS SDK");
    }
}
