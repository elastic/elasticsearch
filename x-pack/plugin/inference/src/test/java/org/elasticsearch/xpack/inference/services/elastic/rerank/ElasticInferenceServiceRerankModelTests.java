/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.rerank;

import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;

public class ElasticInferenceServiceRerankModelTests extends ESTestCase {

    public static ElasticInferenceServiceRerankModel createModel(String url, String modelId) {
        return new ElasticInferenceServiceRerankModel(
            "id",
            TaskType.RERANK,
            "service",
            new ElasticInferenceServiceRerankServiceSettings(modelId, null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            ElasticInferenceServiceComponents.of(url)
        );
    }

}
