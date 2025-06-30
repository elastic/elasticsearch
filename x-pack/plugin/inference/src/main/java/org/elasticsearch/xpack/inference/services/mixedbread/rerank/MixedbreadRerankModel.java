/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadModel;
import org.elasticsearch.xpack.inference.services.mixedbread.action.MixedbreadVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class MixedbreadRerankModel extends MixedbreadModel {
    public static MixedbreadRerankModel of(MixedbreadRerankModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        final var requestTaskSettings = MixedbreadRerankRequestTaskSettings.fromMap(taskSettings);
        final var taskSettingToUse = MixedbreadRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings);

        return new MixedbreadRerankModel(model, taskSettingToUse);
    }

    public MixedbreadRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        MixedbreadRerankServiceSettings serviceSettings,
        MixedbreadRerankTaskSettings taskSettings,
        DefaultSecretSettings secrets
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    public MixedbreadRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            MixedbreadRerankServiceSettings.fromMap(serviceSettings, context),
            MixedbreadRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public MixedbreadRerankModel(MixedbreadRerankModel model, MixedbreadRerankTaskSettings taskSettings) {
        super(model, taskSettings, model.getServiceSettings().rateLimitSettings());
    }

    @Override
    public MixedbreadRerankServiceSettings getServiceSettings() {
        return (MixedbreadRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public MixedbreadRerankTaskSettings getTaskSettings() {
        return (MixedbreadRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return super.getSecretSettings();
    }

    /**
     * Accepts a visitor to create an executable action for this Mixedbread embeddings model.
     *
     * @param creator the visitor that creates the executable action
     * @return an ExecutableAction representing the Mixedbread embeddings model
     */
    public ExecutableAction accept(MixedbreadVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
