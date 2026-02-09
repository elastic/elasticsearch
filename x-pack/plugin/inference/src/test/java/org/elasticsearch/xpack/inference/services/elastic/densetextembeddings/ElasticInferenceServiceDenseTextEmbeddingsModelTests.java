/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.densetextembeddings;

import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceDenseTextEmbeddingsModelTests extends ESTestCase {

    public void testUriCreation() {
        var model = createModel("http://eis-gateway.com", "my-model-id");

        assertThat(model.uri().toString(), is("http://eis-gateway.com/api/v1/embed/text/dense"));
    }

    public void testUriCreation_WithTrailingSlash() {
        var model = createModel("http://eis-gateway.com/", "my-model-id");

        assertThat(model.uri().toString(), is("http://eis-gateway.com/api/v1/embed/text/dense"));
    }

    public static ElasticInferenceServiceDenseTextEmbeddingsModel createModel(String url, String modelId) {
        return new ElasticInferenceServiceDenseTextEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "elastic",
            new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(modelId, SimilarityMeasure.COSINE, null, null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            ElasticInferenceServiceComponents.of(url),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS
        );
    }
}
