/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.cohere.CohereModel;
import org.elasticsearch.xpack.inference.services.cohere.action.CohereActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.Map;

public class CohereCompletionModel extends CohereModel {

    public CohereCompletionModel(
        String modelId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            modelId,
            taskType,
            service,
            CohereCompletionServiceSettings.fromMap(serviceSettings, context),
            EmptyTaskSettings.INSTANCE,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    CohereCompletionModel(
        String modelId,
        TaskType taskType,
        String service,
        CohereCompletionServiceSettings serviceSettings,
        TaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings
        );
    }

    @Override
    public CohereCompletionServiceSettings getServiceSettings() {
        return (CohereCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public TaskSettings getTaskSettings() {
        return super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(CohereActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    @Override
    public URI uri() {
        return getServiceSettings().uri();
    }
}
