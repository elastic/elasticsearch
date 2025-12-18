/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.xpack.inference.TaskTypeTests.randomEmbeddingTaskType;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ConfigurationParseContext.PERSISTENT;
import static org.elasticsearch.xpack.inference.services.ConfigurationParseContext.REQUEST;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.EMBEDDING_TYPE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MULTIMODAL_MODEL;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.BaseJinaAIEmbeddingsServiceSettings.updateEmbeddingDetails;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class BaseJinaAIEmbeddingsServiceSettingsTests extends ESTestCase {

    public void testFromMap_parsesAllFields_textEmbedding_requestContext() {
        testFromMap_parsesAllFields(TEXT_EMBEDDING, REQUEST, randomNonNegativeInt());
    }

    public void testFromMap_parsesAllFields_embedding_requestContext() {
        testFromMap_parsesAllFields(TaskType.EMBEDDING, REQUEST, randomNonNegativeInt());
    }

    public void testFromMap_parsesAllFields_textEmbedding_persistentContext() {
        testFromMap_parsesAllFields(TEXT_EMBEDDING, PERSISTENT, randomNonNegativeInt());
    }

    public void testFromMap_parsesAllFields_embedding_persistentContext() {
        testFromMap_parsesAllFields(TaskType.EMBEDDING, PERSISTENT, randomNonNegativeInt());
    }

    public void testFromMap_parsesAllFields_textEmbedding_requestContext_dimensionsNotSet() {
        testFromMap_parsesAllFields(TEXT_EMBEDDING, REQUEST, null);
    }

    public void testFromMap_parsesAllFields_embedding_requestContext_dimensionsNotSet() {
        testFromMap_parsesAllFields(TaskType.EMBEDDING, REQUEST, null);
    }

    private void testFromMap_parsesAllFields(TaskType taskType, ConfigurationParseContext parseContext, Integer dimensions) {
        var similarity = randomSimilarityMeasure();
        var maxInputTokens = randomNonNegativeInt();
        var model = randomAlphanumericOfLength(8);
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var requestsPerMinute = randomNonNegativeInt();
        var settingsMap = getMapOfCommonEmbeddingSettings(
            model,
            similarity,
            dimensions,
            null,
            maxInputTokens,
            embeddingType,
            requestsPerMinute
        );

        var dimensionsSetByUser = dimensions != null;
        if (parseContext == PERSISTENT) {
            dimensionsSetByUser = randomBoolean();
            settingsMap.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }

        var multimodalModel = false;
        if (taskType == TaskType.EMBEDDING) {
            multimodalModel = randomBoolean();
            settingsMap.put(MULTIMODAL_MODEL, multimodalModel);
        }

        var serviceSettings = BaseJinaAIEmbeddingsServiceSettings.fromMap(settingsMap, taskType, parseContext);

        assertThat(settingsMap, anEmptyMap());

        assertServiceSettings(
            serviceSettings,
            taskType,
            model,
            requestsPerMinute,
            similarity,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser,
            multimodalModel
        );
    }

    public void testFromMap_doesNotRemoveMultimodalModelField_whenTaskTypeIsTextEmbedding() {
        var settingsMap = getMapOfMinimalEmbeddingSettings(randomAlphanumericOfLength(8));

        settingsMap.put(MULTIMODAL_MODEL, randomBoolean());

        var settings = BaseJinaAIEmbeddingsServiceSettings.fromMap(
            settingsMap,
            TEXT_EMBEDDING,
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(settingsMap.get(MULTIMODAL_MODEL), notNullValue());
        assertThat(settings.isMultimodal(), is(false));
    }

    private static void assertServiceSettings(
        BaseJinaAIEmbeddingsServiceSettings serviceSettings,
        TaskType taskType,
        String model,
        Integer requestsPerMinute,
        SimilarityMeasure similarity,
        Integer dimensions,
        Integer maxInputTokens,
        JinaAIEmbeddingType embeddingType,
        boolean dimensionsSetByUser,
        Boolean multimodalModel
    ) {
        BaseJinaAIEmbeddingsServiceSettings expectedSettings;
        if (taskType == TEXT_EMBEDDING) {
            expectedSettings = new JinaAITextEmbeddingServiceSettings(
                new JinaAIServiceSettings(model, new RateLimitSettings(requestsPerMinute)),
                similarity,
                dimensions,
                maxInputTokens,
                embeddingType,
                dimensionsSetByUser
            );
        } else if (taskType == TaskType.EMBEDDING) {
            expectedSettings = new JinaAIEmbeddingServiceSettings(
                new JinaAIServiceSettings(model, new RateLimitSettings(requestsPerMinute)),
                similarity,
                dimensions,
                maxInputTokens,
                embeddingType,
                dimensionsSetByUser,
                multimodalModel
            );
        } else {
            throw new IllegalArgumentException("Invalid taskType " + taskType);
        }

        assertThat(serviceSettings, is(expectedSettings));
    }

    public void testFromMap_withInvalidSimilarity_throwsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> BaseJinaAIEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model", SIMILARITY, similarity)),
                randomEmbeddingTaskType(),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                "Validation Failed: 1: [service_settings] Invalid value [by_size] received. [similarity] "
                    + "must be one of [cosine, dot_product, l2_norm];"
            )
        );
    }

    public void testFromMap_nonPositiveDimensions_throwsError() {
        var dimensions = randomIntBetween(-5, 0);
        var thrownException = expectThrows(
            ValidationException.class,
            () -> BaseJinaAIEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model", DIMENSIONS, dimensions)),
                randomEmbeddingTaskType(),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%d]. [%s] must be a positive integer;",
                    dimensions,
                    DIMENSIONS
                )
            )
        );
    }

    public void testFromMap_withInvalidEmbeddingType_throwsError() {
        var embeddingType = "bad_value";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> BaseJinaAIEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model", EMBEDDING_TYPE, embeddingType)),
                randomEmbeddingTaskType(),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                "Validation Failed: 1: [service_settings] Invalid value [bad_value] received. [embedding_type] "
                    + "must be one of [binary, bit, float];"
            )
        );
    }

    public void testUpdateEmbeddingDetails_returnsSameInstance_whenEmbeddingSizeAndSimilarityAreSame() {
        var settings = randomBoolean()
            ? JinaAIEmbeddingServiceSettingsTests.createRandomWithNoNullValues()
            : JinaAITextEmbeddingServiceSettingsTests.createRandomWithNoNullValues();

        var updatedSettings = updateEmbeddingDetails(settings, settings.dimensions(), settings.similarity());

        assertThat(updatedSettings, sameInstance(settings));
    }

    /**
     * Returns a map containing only the fields that are required by both {@link JinaAIEmbeddingServiceSettings} and
     * {@link JinaAITextEmbeddingServiceSettings}
     */
    public static Map<String, Object> getMapOfMinimalEmbeddingSettings(String modelName) {
        return getMapOfCommonEmbeddingSettings(modelName, null, null, null, null, null, null);
    }

    /**
     * Returns a map containing all fields that are used by both {@link JinaAIEmbeddingServiceSettings} and
     * {@link JinaAITextEmbeddingServiceSettings}
     */
    public static Map<String, Object> getMapOfCommonEmbeddingSettings(
        String modelName,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingType,
        @Nullable Integer requestsPerMinute
    ) {
        var map = JinaAIServiceSettingsTests.getServiceSettingsMap(modelName, requestsPerMinute);
        if (similarity != null) {
            map.put(SIMILARITY, similarity.toString());
        }
        if (dimensions != null) {
            map.put(DIMENSIONS, dimensions);
        }
        if (dimensionsSetByUser != null) {
            map.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }
        if (maxInputTokens != null) {
            map.put(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (embeddingType != null) {
            map.put(EMBEDDING_TYPE, embeddingType.toString());
        }
        return map;
    }
}
