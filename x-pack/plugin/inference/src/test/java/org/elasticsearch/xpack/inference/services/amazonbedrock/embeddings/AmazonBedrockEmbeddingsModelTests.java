/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class AmazonBedrockEmbeddingsModelTests extends ESTestCase {

    public void testCreateModel_withTaskSettings_shouldFail() {
        var baseModel = createModel("id", "region", "model", AmazonBedrockProvider.AMAZONTITAN, "accesskey", "secretkey");
        var thrownException = assertThrows(
            ValidationException.class,
            () -> AmazonBedrockEmbeddingsModel.of(baseModel, Map.of("testkey", "testvalue"))
        );
        assertThat(thrownException.getMessage(), containsString("Amazon Bedrock embeddings model cannot have task settings"));
    }

    // model creation only - no tests to define, but we want to have the public createModel
    // method available

    public static AmazonBedrockEmbeddingsModel createModel(
        String inferenceId,
        String region,
        String model,
        AmazonBedrockProvider provider,
        String accessKey,
        String secretKey
    ) {
        return createModel(inferenceId, region, model, provider, null, false, null, null, new RateLimitSettings(240), accessKey, secretKey);
    }

    public static AmazonBedrockEmbeddingsModel createModel(
        String inferenceId,
        String region,
        String model,
        AmazonBedrockProvider provider,
        @Nullable Integer dimensions,
        boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarity,
        RateLimitSettings rateLimitSettings,
        String accessKey,
        String secretKey
    ) {
        return new AmazonBedrockEmbeddingsModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            "amazonbedrock",
            new AmazonBedrockEmbeddingsServiceSettings(
                region,
                model,
                provider,
                dimensions,
                dimensionsSetByUser,
                maxTokens,
                similarity,
                rateLimitSettings
            ),
            new EmptyTaskSettings(),
            new AmazonBedrockSecretSettings(new SecureString(accessKey), new SecureString(secretKey))
        );
    }
}
