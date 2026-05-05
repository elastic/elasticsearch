/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.custom.request.CompletionParameters;
import org.elasticsearch.xpack.inference.services.custom.request.CustomRequest;
import org.elasticsearch.xpack.inference.services.custom.request.EmbeddingParameters;
import org.elasticsearch.xpack.inference.services.custom.request.RequestParameters;
import org.elasticsearch.xpack.inference.services.custom.request.RerankParameters;

import java.util.List;
import java.util.Map;

/**
 * Creates {@link CustomModel} instances from config maps
 * or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public class CustomModelCreator implements ModelCreator<CustomModel> {
    @Override
    public CustomModel createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        CustomModel model = new CustomModel(
            inferenceId,
            taskType,
            service,
            serviceSettings,
            taskSettings,
            secretSettings,
            chunkingSettings,
            context
        );
        if (context == ConfigurationParseContext.REQUEST) {
            validateConfiguration(model);
        }
        return model;
    }

    /**
     * This does some initial validation with mock inputs to determine if any templates are missing a field to fill them.
     */
    private static void validateConfiguration(CustomModel model) {
        try {
            var request = new CustomRequest(createParameters(model), model);

            // createHttpRequest() takes a listener, but we don't need to do anything with the HttpRequest here, so use a no-op listener
            ActionListener<HttpRequest> listener = ActionListener.noop();
            request.createHttpRequest(listener);
        } catch (IllegalStateException e) {
            var validationException = new ValidationException();
            validationException.addValidationError(Strings.format("Failed to validate model configuration: %s", e.getMessage()));
            throw validationException;
        }
    }

    private static RequestParameters createParameters(CustomModel model) {
        return switch (model.getTaskType()) {
            case RERANK -> RerankParameters.of(new QueryAndDocsInputs("test query", List.of("test input")));
            case COMPLETION -> CompletionParameters.of(new ChatCompletionInput(List.of("test input")));
            case TEXT_EMBEDDING, SPARSE_EMBEDDING -> EmbeddingParameters.of(
                new EmbeddingsInput(() -> List.of(new InferenceStringGroup("test input")), null),
                model.getServiceSettings().getInputTypeTranslator()
            );
            default -> throw new IllegalStateException(
                Strings.format("Unsupported task type [%s] for custom service", model.getTaskType())
            );
        };
    }

    @Override
    public CustomModel createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return new CustomModel(config, secrets);
    }
}
