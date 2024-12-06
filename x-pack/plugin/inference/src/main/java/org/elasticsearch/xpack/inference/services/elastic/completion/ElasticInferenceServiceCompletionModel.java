/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

public class ElasticInferenceServiceCompletionModel extends ElasticInferenceServiceModel {

    public static ElasticInferenceServiceCompletionModel of(
        ElasticInferenceServiceCompletionModel model,
        UnifiedCompletionRequest request
    ) {
        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new ElasticInferenceServiceCompletionServiceSettings(
            Objects.requireNonNullElse(request.model(), originalModelServiceSettings.modelId()),
            originalModelServiceSettings.rateLimitSettings()
        );

        return new ElasticInferenceServiceCompletionModel(model, overriddenServiceSettings);
    }

    private final URI uri;

    public ElasticInferenceServiceCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secrets,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            elasticInferenceServiceComponents
        );
    }

    public ElasticInferenceServiceCompletionModel(
        ElasticInferenceServiceCompletionModel model,
        ElasticInferenceServiceCompletionServiceSettings serviceSettings
    ) {
        super(model, serviceSettings);

        try {
            this.uri = createUri();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    ElasticInferenceServiceCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElasticInferenceServiceSparseEmbeddingsServiceSettings serviceSettings,
        @Nullable TaskSettings taskSettings,
        @Nullable SecretSettings secretSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            serviceSettings,
            elasticInferenceServiceComponents
        );

        try {
            this.uri = createUri();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ElasticInferenceServiceSparseEmbeddingsServiceSettings getServiceSettings() {
        return (ElasticInferenceServiceSparseEmbeddingsServiceSettings) super.getServiceSettings();
    }

    public URI uri() {
        return uri;
    }

    private URI createUri() throws URISyntaxException {
        String modelId = getServiceSettings().modelId();
        // String modelIdUriPath;
        //
        // switch (modelId) {
        // case ElserModels.ELSER_V2_MODEL -> modelIdUriPath = "ELSERv2";
        // default -> throw new IllegalArgumentException(
        // String.format(Locale.ROOT, "Unsupported model for %s [%s]", ELASTIC_INFERENCE_SERVICE_IDENTIFIER, modelId)
        // );
        // }

        // TODO what is the url?
        // return new URI(elasticInferenceServiceComponents().elasticInferenceServiceUrl() + "/api/v1/completion/" + modelId);
        return OpenAiUnifiedChatCompletionRequest.buildDefaultUri();
    }
}
