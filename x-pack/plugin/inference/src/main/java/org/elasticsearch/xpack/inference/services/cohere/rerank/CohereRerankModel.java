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
import org.elasticsearch.xpack.inference.services.cohere.CohereService;
import org.elasticsearch.xpack.inference.services.cohere.action.CohereActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class CohereRerankModel extends CohereModel {
    public static CohereRerankModel of(CohereRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = CohereRerankTaskSettings.fromMap(taskSettings);
        return new CohereRerankModel(model, CohereRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public CohereRerankModel(
        String modelId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            modelId,
            CohereRerankServiceSettings.fromMap(serviceSettings, context),
            CohereRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public CohereRerankModel(
        String modelId,
        CohereRerankServiceSettings serviceSettings,
        CohereRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, TaskType.RERANK, CohereService.NAME, serviceSettings, taskSettings),
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
     * @param visitor _
     * @param taskSettings _
     * @return the rerank action
     */
    @Override
    public ExecutableAction accept(CohereActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
