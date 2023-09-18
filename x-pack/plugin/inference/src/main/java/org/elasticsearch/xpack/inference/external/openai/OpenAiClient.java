/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequest;

import java.io.IOException;

public class OpenAiClient {

    // TODO this should handle throttling and retries
    // TODO maybe we don't need specific request types?

    private final HttpClient httpClient;

    public OpenAiClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public void send(OpenAiEmbeddingsRequest request) throws IOException {
        byte[] body = httpClient.send(request.createRequest());
        ObjectMapper a = new ObjectMapper();

    }
}
