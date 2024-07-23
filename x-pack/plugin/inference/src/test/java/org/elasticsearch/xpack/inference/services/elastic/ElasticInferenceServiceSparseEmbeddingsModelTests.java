/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elser.ElserModels;

public class ElasticInferenceServiceSparseEmbeddingsModelTests extends ESTestCase {

    // TODO: place create model utility methods for other tests here

    public static ElasticInferenceServiceSparseEmbeddingsModel createModel(String url) {
        return new ElasticInferenceServiceSparseEmbeddingsModel(
            randomAlphaOfLength(8),
            TaskType.SPARSE_EMBEDDING,
            randomAlphaOfLength(8),
            new ElasticInferenceServiceSparseEmbeddingsServiceSettings(ElserModels.ELSER_V2_MODEL, null, null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            new ElasticInferenceServiceComponents(url)
        );
    }
}
