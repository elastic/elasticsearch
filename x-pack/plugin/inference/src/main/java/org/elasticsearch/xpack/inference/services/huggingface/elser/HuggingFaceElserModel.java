/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;

public class HuggingFaceElserModel extends Model {
    public HuggingFaceElserModel(
        String modelId,
        TaskType taskType,
        String service,
        HuggingFaceElserServiceSettings serviceSettings,
        HuggingFaceElserSecretSettings secretSettings
    ) {
        super(new ModelConfigurations(modelId, taskType, service, serviceSettings), new ModelSecrets(secretSettings));
    }

    @Override
    public HuggingFaceElserServiceSettings getServiceSettings() {
        return (HuggingFaceElserServiceSettings) super.getServiceSettings();
    }

    @Override
    public HuggingFaceElserSecretSettings getSecretSettings() {
        return (HuggingFaceElserSecretSettings) super.getSecretSettings();
    }
}
