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
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.response.IbmWatsonxErrorResponseEntity;

import static org.elasticsearch.core.Strings.format;

public class IbmWatsonxResponseHandler extends BaseResponseHandler {
    public IbmWatsonxResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction, IbmWatsonxErrorResponseEntity::fromResponse);
    }

    /**
     * Validates the status code and throws a RetryException if it is not in the range [200, 300).
     *
     * The IBM Cloud error codes for text_embedding are loosely
     * defined <a href="https://cloud.ibm.com/apidocs/watsonx-ai#text-embeddings">here</a>.
     * @param request the http request
     * @param result the http response and body
     * @throws RetryException thrown if status code is {@code >= 300 or < 200}
     */
    @Override
    protected void checkForFailureStatusCode(Request request, HttpResult result) throws RetryException {
        if (result.isSuccessfulResponse()) {
            return;
        }

        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500) {
            throw new RetryException(true, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 404) {
            throw new RetryException(false, buildError(resourceNotFoundError(request), request, result));
        } else if (statusCode == 403) {
            throw new RetryException(false, buildError(PERMISSION_DENIED, request, result));
        } else if (statusCode == 401) {
            throw new RetryException(false, buildError(AUTHENTICATION, request, result));
        } else if (statusCode == 400) {
            throw new RetryException(false, buildError(BAD_REQUEST, request, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            throw new RetryException(false, buildError(REDIRECTION, request, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }

    private static String resourceNotFoundError(Request request) {
        return format("Resource not found at [%s]", request.getURI());
    }
}
