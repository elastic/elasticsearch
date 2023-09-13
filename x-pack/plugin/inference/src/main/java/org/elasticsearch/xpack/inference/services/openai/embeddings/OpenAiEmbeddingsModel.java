/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.TaskType;

public class OpenAiEmbeddingsModel extends Model {
    public OpenAiEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        OpenAiEmbeddingsServiceSettings serviceSettings,
        OpenAiEmbeddingsTaskSettings taskSettings
    ) {
        super(modelId, taskType, service, serviceSettings, taskSettings);
    }

    @Override
    public OpenAiEmbeddingsServiceSettings getServiceSettings() {
        return (OpenAiEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public OpenAiEmbeddingsTaskSettings getTaskSettings() {
        return (OpenAiEmbeddingsTaskSettings) super.getTaskSettings();
    }
}
