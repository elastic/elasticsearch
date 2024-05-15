/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;

/**
 * A base class for providing InferenceServiceResults from a response. This is a lightweight wrapper
 * to be able to override the `fromReponse` method to avoid using a static reference to the method.
 */
public abstract class BaseResponseEntity implements ResponseParser {
    protected abstract InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException;

    public InferenceServiceResults apply(Request request, HttpResult response) throws IOException {
        return fromResponse(request, response);
    }
}
