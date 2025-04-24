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
import org.elasticsearch.xpack.inference.services.openai.request.OpenAiEmbeddingsRequest;

import static org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionCreator.EMBEDDINGS_HANDLER;
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModelTests.createModel;

public class OpenAiEmbeddingsRequestManagerTests {
    public static RequestManager makeCreator(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        ThreadPool threadPool
    ) {
        var model = createModel(url, org, apiKey, modelName, user);
        return new TruncatingRequestManager(
            threadPool,
            model,
            EMBEDDINGS_HANDLER,
            (truncationResult) -> new OpenAiEmbeddingsRequest(TruncatorTests.createTruncator(), truncationResult, model),
            null
        );
    }

    public static RequestManager makeCreator(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        String inferenceEntityId,
        ThreadPool threadPool
    ) {
        var model = createModel(url, org, apiKey, modelName, user, inferenceEntityId);
        return new TruncatingRequestManager(
            threadPool,
            model,
            EMBEDDINGS_HANDLER,
            (truncationResult) -> new OpenAiEmbeddingsRequest(TruncatorTests.createTruncator(), truncationResult, model),
            null
        );
    }
}
