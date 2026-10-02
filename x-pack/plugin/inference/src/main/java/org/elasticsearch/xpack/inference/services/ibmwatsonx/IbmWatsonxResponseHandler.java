/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.response.IbmWatsonxErrorResponseEntity;

public class IbmWatsonxResponseHandler extends BaseResponseHandler {
    public IbmWatsonxResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, IbmWatsonxErrorResponseEntity::fromResponse);
    }

    /**
     * Handles failure status codes by returning a RetryException.
     * Only called when the HTTP response status code is not in the range [200, 300).
     *
     * The IBM Cloud error codes for text_embedding are loosely
     * defined <a href="https://cloud.ibm.com/apidocs/watsonx-ai#text-embeddings">here</a>.
     * @param outboundRequest the http request
     * @param result the http response and body
     * @return a RetryException describing the failure
     */
    @Override
    public RetryException buildFailureStatusCodeException(OutboundRequest outboundRequest, HttpResult result) {
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500) {
            return new RetryException(true, buildError(SERVER_ERROR, outboundRequest, result));
        } else if (statusCode == 404) {
            return new RetryException(false, buildError(resourceNotFoundError(outboundRequest), outboundRequest, result));
        } else if (statusCode == 403) {
            return new RetryException(false, buildError(PERMISSION_DENIED, outboundRequest, result));
        } else if (statusCode == 401) {
            return new RetryException(false, buildError(AUTHENTICATION, outboundRequest, result));
        } else if (statusCode == 400) {
            return new RetryException(false, buildError(BAD_REQUEST, outboundRequest, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            return new RetryException(false, buildError(REDIRECTION, outboundRequest, result));
        } else {
            return new RetryException(false, buildError(UNSUCCESSFUL, outboundRequest, result));
        }
    }
}
