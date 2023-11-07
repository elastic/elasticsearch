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

public class OpenAiEmbeddingsModel extends Model {
    public OpenAiEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        OpenAiServiceSettings serviceSettings,
        DefaultSecretSettings secretSettings
    ) {
        super(new ModelConfigurations(modelId, taskType, service, serviceSettings), new ModelSecrets(secretSettings));
    }

    @Override
    public OpenAiServiceSettings getServiceSettings() {
        return (OpenAiServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }
}
