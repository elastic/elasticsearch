/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;

/**
 * TODO - fill in comment
 */
public abstract class EmbeddingResponseEntity implements ResponseParser {
    protected abstract TextEmbeddingResults fromResponse(Request request, HttpResult response) throws IOException;

    public InferenceServiceResults apply(Request request, HttpResult response) throws IOException {
        return fromResponse(request, response);
    }
}
