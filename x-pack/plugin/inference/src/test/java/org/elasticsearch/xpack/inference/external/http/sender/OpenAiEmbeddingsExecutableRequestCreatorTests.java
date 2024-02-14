/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.common.TruncatorTests;

import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModelTests.createModel;

public class OpenAiEmbeddingsExecutableRequestCreatorTests {
    public static OpenAiEmbeddingsExecutableRequestCreator makeCreator(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user
    ) {
        var model = createModel(url, org, apiKey, modelName, user);

        return new OpenAiEmbeddingsExecutableRequestCreator(model, TruncatorTests.createTruncator());
    }

    public static OpenAiEmbeddingsExecutableRequestCreator makeCreator(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        String inferenceEntityId
    ) {
        var model = createModel(url, org, apiKey, modelName, user, inferenceEntityId);

        return new OpenAiEmbeddingsExecutableRequestCreator(model, TruncatorTests.createTruncator());
    }
}
