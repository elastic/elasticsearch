/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AmazonBedrockEmbeddingsModelTests extends ESTestCase {

    public void testCreateModel_withTaskSettingsOverride() throws IOException {
        var baseTaskSettings = AmazonBedrockEmbeddingsTaskSettingsTests.randomTaskSettings();
        var baseModel = createModel("id", "region", "model", AmazonBedrockProvider.AMAZONTITAN, "accesskey", "secretkey", baseTaskSettings);

        var overrideTaskSettings = AmazonBedrockEmbeddingsTaskSettingsTests.mutateTaskSettings(baseTaskSettings);
        var overrideTaskSettingsMap = AmazonBedrockEmbeddingsTaskSettingsTests.toMap(overrideTaskSettings);

        var overriddenModel = AmazonBedrockEmbeddingsModel.of(baseModel, overrideTaskSettingsMap);
        assertThat(overriddenModel.getTaskSettings(), equalTo(overrideTaskSettings));
        assertThat(overriddenModel.getTaskSettings(), not(equalTo(baseTaskSettings)));
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
        return createModel(
            inferenceId,
            region,
            model,
            provider,
            accessKey,
            secretKey,
            AmazonBedrockEmbeddingsTaskSettingsTests.emptyTaskSettings()
        );
    }

    public static AmazonBedrockEmbeddingsModel createModel(
        String inferenceId,
        String region,
        String model,
        AmazonBedrockProvider provider,
        String accessKey,
        String secretKey,
        AmazonBedrockEmbeddingsTaskSettings taskSettings
    ) {
        return createModel(
            inferenceId,
            region,
            model,
            provider,
            null,
            false,
            null,
            null,
            new RateLimitSettings(240),
            accessKey,
            secretKey,
            taskSettings
        );
    }

    public static AmazonBedrockEmbeddingsModel createModel(
        String inferenceId,
        String region,
        String model,
        AmazonBedrockProvider provider,
        ChunkingSettings chunkingSettings,
        String accessKey,
        String secretKey
    ) {
        return createModel(
            inferenceId,
            region,
            model,
            provider,
            null,
            false,
            null,
            null,
            new RateLimitSettings(240),
            chunkingSettings,
            accessKey,
            secretKey
        );
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
        ChunkingSettings chunkingSettings,
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
            AmazonBedrockEmbeddingsTaskSettingsTests.emptyTaskSettings(),
            chunkingSettings,
            new AwsSecretSettings(new SecureString(accessKey), new SecureString(secretKey))
        );
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
        return createModel(
            inferenceId,
            region,
            model,
            provider,
            dimensions,
            dimensionsSetByUser,
            maxTokens,
            similarity,
            rateLimitSettings,
            accessKey,
            secretKey,
            AmazonBedrockEmbeddingsTaskSettingsTests.emptyTaskSettings()
        );
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
        String secretKey,
        AmazonBedrockEmbeddingsTaskSettings taskSettings
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
            taskSettings,
            null,
            new AwsSecretSettings(new SecureString(accessKey), new SecureString(secretKey))
        );
    }
}
