/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettingsTests;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.EMBEDDING_TYPE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.BaseJinaAIEmbeddingsServiceSettings.updateEmbeddingDetails;
import static org.hamcrest.Matchers.sameInstance;

public class BaseJinaAIEmbeddingsServiceSettingsTests extends ESTestCase {

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
