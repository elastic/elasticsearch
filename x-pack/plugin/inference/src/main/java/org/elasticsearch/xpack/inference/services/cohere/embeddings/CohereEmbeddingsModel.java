/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.cohere.CohereModel;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class CohereEmbeddingsModel extends CohereModel {
    public CohereEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets
    ) {
        this(
            modelId,
            taskType,
            service,
            CohereServiceSettings.fromMap(serviceSettings),
            CohereEmbeddingsTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    CohereEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        CohereServiceSettings serviceSettings,
        CohereEmbeddingsTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secretSettings));
    }

    private CohereEmbeddingsModel(CohereEmbeddingsModel model, CohereEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    private CohereEmbeddingsModel(CohereEmbeddingsModel model, CohereServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public CohereServiceSettings getServiceSettings() {
        return (CohereServiceSettings) super.getServiceSettings();
    }

    @Override
    public CohereEmbeddingsTaskSettings getTaskSettings() {
        return (CohereEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept() {
        return null;
    }
}
