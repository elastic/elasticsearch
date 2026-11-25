/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceResponseHandler;

/**
 * Response handler for document extraction requests. A 413 (content too large) is not retried, because the default behavior
 * of truncating the request makes no sense for a document: a truncated document would silently yield partial extraction results.
 */
public class ElasticInferenceServiceDocumentExtractionResponseHandler extends ElasticInferenceServiceResponseHandler {

    private static final int CONTENT_TOO_LARGE_STATUS_CODE = 413;

    public ElasticInferenceServiceDocumentExtractionResponseHandler(String requestType, ResponseParser parseFunction) {
        super(requestType, parseFunction);
    }

    @Override
    public RetryException buildFailureStatusCodeException(OutboundRequest outboundRequest, HttpResult result) {
        if (result.response().getCode() == CONTENT_TOO_LARGE_STATUS_CODE) {
            return new RetryException(false, buildError(CONTENT_TOO_LARGE, outboundRequest, result));
        }
        return super.buildFailureStatusCodeException(outboundRequest, result);
    }
}
