/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.cohere.CohereModel;
import org.elasticsearch.xpack.inference.services.cohere.action.CohereActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.Map;

public class CohereRerankModel extends CohereModel {
    public static CohereRerankModel of(CohereRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = CohereRerankTaskSettings.fromMap(taskSettings);
        return new CohereRerankModel(model, CohereRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public CohereRerankModel(
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
            CohereRerankServiceSettings.fromMap(serviceSettings, context),
            CohereRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    CohereRerankModel(
        String modelId,
        TaskType taskType,
        String service,
        CohereRerankServiceSettings serviceSettings,
        CohereRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings
        );
    }

    private CohereRerankModel(CohereRerankModel model, CohereRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    public CohereRerankModel(CohereRerankModel model, CohereRerankServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public CohereRerankServiceSettings getServiceSettings() {
        return (CohereRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public CohereRerankTaskSettings getTaskSettings() {
        return (CohereRerankTaskSettings) super.getTaskSettings();
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
    public ExecutableAction accept(CohereActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    @Override
    public URI uri() {
        return getServiceSettings().uri();
    }
}
