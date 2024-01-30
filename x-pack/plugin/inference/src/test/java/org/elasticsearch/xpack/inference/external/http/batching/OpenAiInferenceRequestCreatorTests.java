/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.openai.OpenAiResponseHandler;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenAiInferenceRequestCreatorTests extends ESTestCase {

    public static OpenAiEmbeddingsRequestCreator create(OpenAiEmbeddingsModel model) {
        return new OpenAiEmbeddingsRequestCreator(
            model,
            new OpenAiResponseHandler("openai text embedding", OpenAiEmbeddingsResponseEntity::fromResponse),
            TruncatorTests.createTruncator()
        );
    }

    public static OpenAiEmbeddingsRequestCreator createMock() {
        var mockCreator = mock(OpenAiEmbeddingsRequestCreator.class);
        when(mockCreator.createRequest(any(), any(), any(), any())).thenReturn(() -> {});

        return mockCreator;
    }
}
