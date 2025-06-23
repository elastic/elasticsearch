/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.densetextembeddings;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

public class ElasticInferenceServiceDenseTextEmbeddingsModelTests {

    public static ElasticInferenceServiceDenseTextEmbeddingsModel createModel(
        String url,
        String modelId,
        ChunkingSettings chunkingSettings
    ) {
        return new ElasticInferenceServiceDenseTextEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "elastic",
            new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
                modelId,
                SimilarityMeasure.COSINE,
                null,
                null,
                new RateLimitSettings(1000L)
            ),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            ElasticInferenceServiceComponents.of(url)
        );
    }

}
