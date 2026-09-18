/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.rerank;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiModel;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiSecretSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiUtils;
import org.elasticsearch.xpack.inference.services.ocigenai.action.OciGenAiActionVisitor;

import java.util.Map;
import java.util.function.BiConsumer;

public class OciGenAiRerankModel extends OciGenAiModel {

    public static OciGenAiRerankModel of(OciGenAiRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = OciGenAiRerankTaskSettings.fromMap(taskSettings);
        return new OciGenAiRerankModel(model, OciGenAiRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public OciGenAiRerankModel(
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
            OciGenAiRerankServiceSettings.fromMap(serviceSettings, context),
            OciGenAiRerankTaskSettings.fromMap(taskSettings),
            OciGenAiSecretSettings.fromMap(secrets, context)
        );
    }

    public OciGenAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        OciGenAiRerankServiceSettings serviceSettings,
        OciGenAiRerankTaskSettings taskSettings,
        @Nullable OciGenAiSecretSettings secrets
    ) {
        this(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    public OciGenAiRerankModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets, OciGenAiUtils.RERANK_TEXT);
    }

    // Should only be used directly for testing. Allows overriding the URL and bypassing request signing.
    public OciGenAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        @Nullable String url,
        OciGenAiRerankServiceSettings serviceSettings,
        OciGenAiRerankTaskSettings taskSettings,
        @Nullable OciGenAiSecretSettings secrets,
        BiConsumer<HttpPost, OciGenAiModel> requestSigner
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            OciGenAiUtils.RERANK_TEXT,
            url,
            requestSigner
        );
    }

    private OciGenAiRerankModel(OciGenAiRerankModel model, OciGenAiRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public OciGenAiRerankServiceSettings getServiceSettings() {
        return (OciGenAiRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public OciGenAiRerankTaskSettings getTaskSettings() {
        return (OciGenAiRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public ExecutableAction accept(OciGenAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
