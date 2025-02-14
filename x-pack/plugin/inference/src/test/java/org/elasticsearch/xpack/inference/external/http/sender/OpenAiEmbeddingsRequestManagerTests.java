/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.TruncatorTests;

import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModelTests.createModel;

public class OpenAiEmbeddingsRequestManagerTests {
    public static OpenAiEmbeddingsRequestManager makeCreator(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        ThreadPool threadPool
    ) {
        var model = createModel(url, org, apiKey, modelName, user);

        return OpenAiEmbeddingsRequestManager.of(model, TruncatorTests.createTruncator(), threadPool);
    }

    public static OpenAiEmbeddingsRequestManager makeCreator(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        String inferenceEntityId,
        ThreadPool threadPool
    ) {
        var model = createModel(url, org, apiKey, modelName, user, inferenceEntityId);

        return OpenAiEmbeddingsRequestManager.of(model, TruncatorTests.createTruncator(), threadPool);
    }
}
