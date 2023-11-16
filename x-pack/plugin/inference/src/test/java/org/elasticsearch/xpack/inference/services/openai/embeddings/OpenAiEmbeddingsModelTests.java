/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsRequestTaskSettingsTests.getRequestTaskSettingsMap;
import static org.hamcrest.Matchers.is;

public class OpenAiEmbeddingsModelTests extends ESTestCase {

    public void testOverrideWith_OverridesUser() {
        var model = createModel("url", "org", "api_key", "model_name", null);
        var requestTaskSettingsMap = getRequestTaskSettingsMap(null, "user_override");

        var overriddenModel = model.overrideWith(requestTaskSettingsMap);

        assertThat(overriddenModel, is(createModel("url", "org", "api_key", "model_name", "user_override")));
    }

    public static OpenAiEmbeddingsModel createModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user
    ) {
        return new OpenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new OpenAiServiceSettings(url, org),
            new OpenAiEmbeddingsTaskSettings(modelName, user),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
