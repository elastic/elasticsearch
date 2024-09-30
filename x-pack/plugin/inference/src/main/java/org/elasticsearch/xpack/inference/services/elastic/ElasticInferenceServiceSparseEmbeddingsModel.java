/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.elastic.ElasticInferenceServiceActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elser.ElserModels;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class ElasticInferenceServiceSparseEmbeddingsModel extends ElasticInferenceServiceModel {

    private final URI uri;

    public ElasticInferenceServiceSparseEmbeddingsModel(
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

    public ElasticInferenceServiceSparseEmbeddingsModel(
        ElasticInferenceServiceSparseEmbeddingsModel model,
        ElasticInferenceServiceSparseEmbeddingsServiceSettings serviceSettings
    ) {
        super(model, serviceSettings);

        try {
            this.uri = createUri();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    ElasticInferenceServiceSparseEmbeddingsModel(
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
    public ExecutableAction accept(ElasticInferenceServiceActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this);
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
        String modelIdUriPath;

        switch (modelId) {
            case ElserModels.ELSER_V2_MODEL -> modelIdUriPath = "ELSERv2";
            default -> throw new IllegalArgumentException("Unsupported model for EIS [" + modelId + "]");
        }

        return new URI(elasticInferenceServiceComponents().eisGatewayUrl() + "/sparse-text-embedding/" + modelIdUriPath);
    }
}
