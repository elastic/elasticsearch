/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class OpenAiEmbeddingsModel2 extends Model {
    public OpenAiEmbeddingsModel2(
        String modelId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secrets
    ) {
        super(
            new ModelConfigurations(
                modelId,
                taskType,
                service,
                OpenAiServiceSettings.fromMap(serviceSettings),
                OpenAiEmbeddingsTaskSettings.fromMap(taskSettings)
            ),
            new ModelSecrets(DefaultSecretSettings.fromMap(secrets))
        );
    }

    @Override
    public OpenAiServiceSettings getServiceSettings() {
        return (OpenAiServiceSettings) super.getServiceSettings();
    }

    @Override
    public OpenAiEmbeddingsTaskSettings getTaskSettings() {
        return (OpenAiEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }
}
