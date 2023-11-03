/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;
import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForFailureStatusCode;

public abstract class DefaultResponseHandler implements ResponseHandler {
    protected final String requestType;

    public DefaultResponseHandler(String requestType) {
        this.requestType = Objects.requireNonNull(requestType);
    }

    public void validateResponse(ThrottlerManager throttlerManager, Logger logger, HttpRequestBase request, HttpResult result) {
        checkForFailureStatusCode(throttlerManager, logger, request, result);
        checkForEmptyBody(throttlerManager, logger, request, result);
    }

    public String getRequestType() {
        return requestType;
    }
}
