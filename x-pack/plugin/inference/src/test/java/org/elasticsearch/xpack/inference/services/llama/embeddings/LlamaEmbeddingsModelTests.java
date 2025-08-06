/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsRequestTaskSettingsTests.createRequestTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class LlamaEmbeddingsModelTests extends ESTestCase {
    public static LlamaEmbeddingsModel createEmbeddingsModel(
        String modelId,
        String url,
        String apiKey,
        String user,
        Integer dimensions,
        boolean dimensionsSetByUser
    ) {
        return new LlamaEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "llama",
            new LlamaEmbeddingsServiceSettings(modelId, url, dimensions, null, null, dimensionsSetByUser, null),
            new OpenAiEmbeddingsTaskSettings(user),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static LlamaEmbeddingsModel createEmbeddingsModelWithChunkingSettings(String modelId, String url, String apiKey, String user) {
        return new LlamaEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "llama",
            new LlamaEmbeddingsServiceSettings(modelId, url, null, null, null, false, null),
            new OpenAiEmbeddingsTaskSettings(user),
            createRandomChunkingSettings(),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static LlamaEmbeddingsModel createEmbeddingsModelNoAuth(String modelId, String url, String user) {
        return new LlamaEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "llama",
            new LlamaEmbeddingsServiceSettings(modelId, url, null, null, null, false, null),
            new OpenAiEmbeddingsTaskSettings(user),
            null,
            EmptySecretSettings.INSTANCE
        );
    }

    public void testOverrideWith_OverridesUser() {
        var model = createEmbeddingsModel("model_name", "url", "api_key", "user", null, false);
        var requestTaskSettingsMap = createRequestTaskSettingsMap("user_override");

        var overriddenModel = LlamaEmbeddingsModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, is(createEmbeddingsModel("model_name", "url", "api_key", "user_override", null, false)));
    }

    public void testOverrideWith_OverridesNullUser() {
        var model = createEmbeddingsModel("model_name", "url", "api_key", null, null, false);
        var requestTaskSettingsMap = createRequestTaskSettingsMap("user_override");

        var overriddenModel = LlamaEmbeddingsModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, is(createEmbeddingsModel("model_name", "url", "api_key", "user_override", null, false)));
    }

    public void testOverrideWith_EmptyMap() {
        var model = createEmbeddingsModel("model_name", "url", "api_key", "user", null, false);

        var requestTaskSettingsMap = Map.<String, Object>of();

        var overriddenModel = LlamaEmbeddingsModel.of(model, requestTaskSettingsMap);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_NullMap() {
        var model = createEmbeddingsModel("model_name", "url", "api_key", "user", null, false);

        var overriddenModel = LlamaEmbeddingsModel.of(model, null);
        assertThat(overriddenModel, sameInstance(model));
    }
}
