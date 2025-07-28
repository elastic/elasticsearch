/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AmazonBedrockChatCompletionModelTests extends ESTestCase {
    public void testOverrideWith_OverridesWithoutValues() {
        var model = createModel(
            "id",
            "region",
            "model",
            AmazonBedrockProvider.AMAZONTITAN,
            1.0,
            0.5,
            0.6,
            512,
            null,
            "access_key",
            "secret_key"
        );
        var requestTaskSettingsMap = getChatCompletionTaskSettingsMap(null, null, null, null);
        var overriddenModel = AmazonBedrockChatCompletionModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, sameInstance(overriddenModel));
    }

    public void testOverrideWith_temperature() {
        var model = createModel(
            "id",
            "region",
            "model",
            AmazonBedrockProvider.AMAZONTITAN,
            1.0,
            null,
            null,
            null,
            null,
            "access_key",
            "secret_key"
        );
        var requestTaskSettings = getChatCompletionTaskSettingsMap(0.5, null, null, null);
        var overriddenModel = AmazonBedrockChatCompletionModel.of(model, requestTaskSettings);
        assertThat(
            overriddenModel,
            is(
                createModel(
                    "id",
                    "region",
                    "model",
                    AmazonBedrockProvider.AMAZONTITAN,
                    0.5,
                    null,
                    null,
                    null,
                    null,
                    "access_key",
                    "secret_key"
                )
            )
        );
    }

    public void testOverrideWith_topP() {
        var model = createModel(
            "id",
            "region",
            "model",
            AmazonBedrockProvider.AMAZONTITAN,
            null,
            0.8,
            null,
            null,
            null,
            "access_key",
            "secret_key"
        );
        var requestTaskSettings = getChatCompletionTaskSettingsMap(null, 0.5, null, null);
        var overriddenModel = AmazonBedrockChatCompletionModel.of(model, requestTaskSettings);
        assertThat(
            overriddenModel,
            is(
                createModel(
                    "id",
                    "region",
                    "model",
                    AmazonBedrockProvider.AMAZONTITAN,
                    null,
                    0.5,
                    null,
                    null,
                    null,
                    "access_key",
                    "secret_key"
                )
            )
        );
    }

    public void testOverrideWith_topK() {
        var model = createModel(
            "id",
            "region",
            "model",
            AmazonBedrockProvider.AMAZONTITAN,
            null,
            null,
            1.0,
            null,
            null,
            "access_key",
            "secret_key"
        );
        var requestTaskSettings = getChatCompletionTaskSettingsMap(null, null, 0.8, null);
        var overriddenModel = AmazonBedrockChatCompletionModel.of(model, requestTaskSettings);
        assertThat(
            overriddenModel,
            is(
                createModel(
                    "id",
                    "region",
                    "model",
                    AmazonBedrockProvider.AMAZONTITAN,
                    null,
                    null,
                    0.8,
                    null,
                    null,
                    "access_key",
                    "secret_key"
                )
            )
        );
    }

    public void testOverrideWith_maxNewTokens() {
        var model = createModel(
            "id",
            "region",
            "model",
            AmazonBedrockProvider.AMAZONTITAN,
            null,
            null,
            null,
            512,
            null,
            "access_key",
            "secret_key"
        );
        var requestTaskSettings = getChatCompletionTaskSettingsMap(null, null, null, 128);
        var overriddenModel = AmazonBedrockChatCompletionModel.of(model, requestTaskSettings);
        assertThat(
            overriddenModel,
            is(
                createModel(
                    "id",
                    "region",
                    "model",
                    AmazonBedrockProvider.AMAZONTITAN,
                    null,
                    null,
                    null,
                    128,
                    null,
                    "access_key",
                    "secret_key"
                )
            )
        );
    }

    public static AmazonBedrockChatCompletionModel createModel(
        String id,
        String region,
        String model,
        AmazonBedrockProvider provider,
        String accessKey,
        String secretKey
    ) {
        return createModel(id, region, model, provider, null, null, null, null, null, accessKey, secretKey);
    }

    public static AmazonBedrockChatCompletionModel createModel(
        String id,
        String region,
        String model,
        AmazonBedrockProvider provider,
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Double topK,
        @Nullable Integer maxNewTokens,
        @Nullable RateLimitSettings rateLimitSettings,
        String accessKey,
        String secretKey
    ) {
        return new AmazonBedrockChatCompletionModel(
            id,
            TaskType.COMPLETION,
            "amazonbedrock",
            new AmazonBedrockChatCompletionServiceSettings(region, model, provider, rateLimitSettings),
            new AmazonBedrockChatCompletionTaskSettings(temperature, topP, topK, maxNewTokens),
            new AwsSecretSettings(new SecureString(accessKey), new SecureString(secretKey))
        );
    }

}
