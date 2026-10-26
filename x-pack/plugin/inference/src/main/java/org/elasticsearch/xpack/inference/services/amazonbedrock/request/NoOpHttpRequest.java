/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;

/**
 * Needed for compatibility with RequestSender
 */
public final class NoOpHttpRequest {

    /**
     * Creates a do-nothing request; Amazon Bedrock uses the AWS SDK rather than Apache HTTP requests,
     * so this request is never executed.
     */
    public static SimpleHttpRequest createNoOpRequest() {
        return SimpleRequestBuilder.create("NOOP").build();
    }

    private NoOpHttpRequest() {}
}
