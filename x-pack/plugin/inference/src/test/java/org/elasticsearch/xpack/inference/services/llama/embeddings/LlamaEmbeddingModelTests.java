/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

public class LlamaEmbeddingModelTests extends ESTestCase {
    public static LlamaEmbeddingsModel createEmbeddingsModel(String modelId, String url, String apiKey) {
        return new LlamaEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "llama",
            new LlamaEmbeddingsServiceSettings(modelId, url, null, null, null, null),
            EmptyTaskSettings.INSTANCE,
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
