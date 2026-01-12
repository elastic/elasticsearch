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
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadService;
import org.elasticsearch.xpack.inference.services.mixedbread.action.MixedbreadActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class MixedbreadRerankModel extends MixedbreadModel {
    public static MixedbreadRerankModel of(MixedbreadRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = MixedbreadRerankTaskSettings.fromMap(taskSettings);
        return new MixedbreadRerankModel(model, MixedbreadRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public MixedbreadRerankModel(
        String modelId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            modelId,
            MixedbreadRerankServiceSettings.fromMap(serviceSettings, context),
            MixedbreadRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public MixedbreadRerankModel(
        String modelId,
        MixedbreadRerankServiceSettings serviceSettings,
        MixedbreadRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, TaskType.RERANK, MixedbreadService.NAME, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings
        );
    }

    private MixedbreadRerankModel(MixedbreadRerankModel model, MixedbreadRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    public MixedbreadRerankModel(MixedbreadRerankModel model, MixedbreadRerankServiceSettings serviceSettings) {
        super(model, serviceSettings);
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
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    /**
     * Accepts a visitor to create an executable action. The returned action will not return documents in the response.
     * @param visitor          Interface for creating {@link ExecutableAction} instances for Cohere models.
     * @param taskSettings     Settings in the request to override the model's defaults
     * @return the rerank action
     */
    @Override
    public ExecutableAction accept(MixedbreadActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
