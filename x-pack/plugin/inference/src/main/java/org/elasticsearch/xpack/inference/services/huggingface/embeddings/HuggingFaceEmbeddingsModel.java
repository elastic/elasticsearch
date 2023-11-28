/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.embeddings;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.huggingface.HuggingFaceActionVisitor;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class HuggingFaceEmbeddingsModel extends HuggingFaceModel {
    public HuggingFaceEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> secrets
    ) {
        this(modelId, taskType, service, HuggingFaceServiceSettings.fromMap(serviceSettings), DefaultSecretSettings.fromMap(secrets));
    }

    // Should only be used directly for testing
    HuggingFaceEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        HuggingFaceServiceSettings serviceSettings,
        DefaultSecretSettings secrets
    ) {
        super(new ModelConfigurations(modelId, taskType, service, serviceSettings), new ModelSecrets(secrets));
    }

    @Override
    public HuggingFaceServiceSettings getServiceSettings() {
        return (HuggingFaceServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(HuggingFaceActionVisitor creator) {
        return creator.create(this);
    }
}
