/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.rerank;

import org.apache.hc.core5.net.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIModel;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIService;
import org.elasticsearch.xpack.inference.services.jinaai.action.JinaAIActionVisitor;
import org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIUtils;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public class JinaAIRerankModel extends JinaAIModel {

    private static final URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme("https")
        .setHost(JinaAIUtils.HOST)
        .setPathSegments(JinaAIUtils.VERSION_1, JinaAIUtils.RERANK_PATH);

    public static JinaAIRerankModel of(JinaAIRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = JinaAIRerankTaskSettings.fromMap(taskSettings);
        if (requestTaskSettings.isEmpty() || requestTaskSettings.equals(model.getTaskSettings())) {
            return model;
        }
        return new JinaAIRerankModel(model, JinaAIRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public JinaAIRerankModel(
        String inferenceId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            JinaAIRerankServiceSettings.fromMap(serviceSettings, context),
            JinaAIRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets, context),
            null
        );
    }

    public JinaAIRerankModel(
        String modelId,
        JinaAIRerankServiceSettings serviceSettings,
        JinaAIRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings,
        @Nullable String uri
    ) {
        super(
            new ModelConfigurations(modelId, TaskType.RERANK, JinaAIService.NAME, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            Objects.requireNonNullElse(ServiceUtils.createOptionalUri(uri), buildUri(JinaAIService.NAME, DEFAULT_URI_BUILDER::build))
        );
    }

    public JinaAIRerankModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets, buildUri(JinaAIService.NAME, DEFAULT_URI_BUILDER::build));
    }

    private JinaAIRerankModel(JinaAIRerankModel model, JinaAIRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public JinaAIRerankServiceSettings getServiceSettings() {
        return (JinaAIRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public JinaAIRerankTaskSettings getTaskSettings() {
        return (JinaAIRerankTaskSettings) super.getTaskSettings();
    }

    /**
     * Accepts a visitor to create an executable action. The returned action will not return documents in the response.
     * @param visitor          Interface for creating {@link ExecutableAction} instances for Jina AI models.
     * @param taskSettings     Settings in the request to override the model's defaults
     * @return the rerank action
     */
    @Override
    public ExecutableAction accept(JinaAIActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
