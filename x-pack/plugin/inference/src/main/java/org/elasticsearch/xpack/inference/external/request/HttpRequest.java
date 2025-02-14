/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;

import java.util.Objects;

/**
 * Provides a thin wrapper to give access the inference entity id that manages the settings for this request.
 */
public record HttpRequest(HttpRequestBase httpRequestBase, String inferenceEntityId) {
    public HttpRequest {
        Objects.requireNonNull(httpRequestBase);
        Objects.requireNonNull(inferenceEntityId);
    }

    public HttpAsyncRequestProducer requestProducer() {
        return HttpAsyncMethods.create(httpRequestBase);
    }
}
