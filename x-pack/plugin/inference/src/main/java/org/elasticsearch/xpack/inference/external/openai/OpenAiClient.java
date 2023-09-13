/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequest;

public class OpenAiClient {
    // TODO this should handle sending the request

    // TODO this needs an http client
    // TODO this should handle throttling and retries
    // TODO maybe this won't have an account instance so it shouldn't handle building the request

    // TODO maybe we don't need specific request types?

    private final HttpClient httpClient;

    public OpenAiClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public void send(OpenAiEmbeddingsRequest request) {

    }
}
