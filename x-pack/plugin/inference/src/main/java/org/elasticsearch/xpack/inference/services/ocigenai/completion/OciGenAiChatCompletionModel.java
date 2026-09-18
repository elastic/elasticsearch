/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.completion;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiModel;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiSecretSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiUtils;
import org.elasticsearch.xpack.inference.services.ocigenai.action.OciGenAiActionVisitor;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Model of the OCI Generative AI {@code completion} and {@code chat_completion} tasks, both served by the {@code chat} action.
 */
public class OciGenAiChatCompletionModel extends OciGenAiModel {

    /**
     * Creates a copy of the model targeting the model id of the request when the request specifies one.
     */
    public static OciGenAiChatCompletionModel of(OciGenAiChatCompletionModel model, UnifiedCompletionRequestBody request) {
        if (request.model() == null || request.model().equals(model.getServiceSettings().modelId())) {
            return model;
        }

        var overriddenServiceSettings = new OciGenAiChatCompletionServiceSettings(
            model.getServiceSettings().common().withModelId(request.model())
        );
        return new OciGenAiChatCompletionModel(model, overriddenServiceSettings);
    }

    public OciGenAiChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            OciGenAiChatCompletionServiceSettings.fromMap(serviceSettings, context),
            OciGenAiSecretSettings.fromMap(secrets, context)
        );
    }

    public OciGenAiChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        OciGenAiChatCompletionServiceSettings serviceSettings,
        @Nullable OciGenAiSecretSettings secrets
    ) {
        this(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings), new ModelSecrets(secrets));
    }

    public OciGenAiChatCompletionModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets, OciGenAiUtils.CHAT);
    }

    // Should only be used directly for testing. Allows overriding the URL and bypassing request signing.
    public OciGenAiChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        @Nullable String url,
        OciGenAiChatCompletionServiceSettings serviceSettings,
        @Nullable OciGenAiSecretSettings secrets,
        BiConsumer<HttpPost, OciGenAiModel> requestSigner
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE),
            new ModelSecrets(secrets),
            OciGenAiUtils.CHAT,
            url,
            requestSigner
        );
    }

    private OciGenAiChatCompletionModel(OciGenAiChatCompletionModel model, OciGenAiChatCompletionServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public OciGenAiChatCompletionServiceSettings getServiceSettings() {
        return (OciGenAiChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public ExecutableAction accept(OciGenAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this);
    }
}
