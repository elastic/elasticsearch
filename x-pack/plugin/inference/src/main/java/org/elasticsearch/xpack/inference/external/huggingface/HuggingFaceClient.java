/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.huggingface;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequest;

import java.io.IOException;

public class HuggingFaceClient {
    /**
     * It is expensive to construct the ObjectMapper so we'll do it once
     * <a href="https://github.com/FasterXML/jackson-docs/wiki/Presentation:-Jackson-Performance">See here for more details</a>
     */
    // TODO move this to the plugin
    private static final ObjectMapper mapper = new ObjectMapper();

    private final Sender sender;

    public HuggingFaceClient(Sender sender) {
        this.sender = sender;
    }

    public void send(HuggingFaceElserRequest request, ActionListener<InferenceResults> listener) throws IOException {
        ActionListener<HttpResult> responseListener = ActionListener.wrap(response -> {
            try {
                listener.onResponse(OpenAiEmbeddingsResponseEntity.fromResponse(mapper, response));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);

        sender.send(request.createRequest(), responseListener);
    }
}
