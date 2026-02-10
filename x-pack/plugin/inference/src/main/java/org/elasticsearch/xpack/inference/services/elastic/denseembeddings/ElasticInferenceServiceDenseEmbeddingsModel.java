/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.denseembeddings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class ElasticInferenceServiceDenseEmbeddingsModel extends ElasticInferenceServiceModel {

    public static final String TEXT_EMBEDDING_PATH = "/api/v1/embed/text/dense";
    public static final String MULTIMODAL_EMBEDDING_PATH = "/api/v1/embed/dense";
    private final URI uri;

    public ElasticInferenceServiceDenseEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
        ConfigurationParseContext context,
        ChunkingSettings chunkingSettings,
        @Nullable EndpointMetadata endpointMetadata
    ) {
        this(
            inferenceEntityId,
            taskType,
            ElasticInferenceServiceDenseEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            elasticInferenceServiceComponents,
            chunkingSettings,
            endpointMetadata
        );
    }

    public ElasticInferenceServiceDenseEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        ElasticInferenceServiceDenseEmbeddingsServiceSettings serviceSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
        ChunkingSettings chunkingSettings
    ) {
        this(inferenceEntityId, taskType, serviceSettings, elasticInferenceServiceComponents, chunkingSettings, null);
    }

    public ElasticInferenceServiceDenseEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        ElasticInferenceServiceDenseEmbeddingsServiceSettings serviceSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
        ChunkingSettings chunkingSettings,
        @Nullable EndpointMetadata endpointMetadata
    ) {
        this(
            new ModelConfigurations(
                inferenceEntityId,
                taskType,
                ElasticInferenceService.NAME,
                serviceSettings,
                EmptyTaskSettings.INSTANCE,
                chunkingSettings,
                endpointMetadata
            ),
            ModelSecrets.emptySecrets(),
            elasticInferenceServiceComponents
        );
    }

    public ElasticInferenceServiceDenseEmbeddingsModel(
        ModelConfigurations modelConfigurations,
        ModelSecrets modelSecrets,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents
    ) {
        super(
            modelConfigurations,
            modelSecrets,
            (ElasticInferenceServiceDenseEmbeddingsServiceSettings) modelConfigurations.getServiceSettings(),
            elasticInferenceServiceComponents
        );
        this.uri = createUri();
    }

    public ElasticInferenceServiceDenseEmbeddingsModel(
        ElasticInferenceServiceDenseEmbeddingsModel model,
        ElasticInferenceServiceDenseEmbeddingsServiceSettings serviceSettings
    ) {
        super(model, serviceSettings);
        this.uri = createUri();
    }

    @Override
    public ElasticInferenceServiceDenseEmbeddingsServiceSettings getServiceSettings() {
        return (ElasticInferenceServiceDenseEmbeddingsServiceSettings) super.getServiceSettings();
    }

    public URI uri() {
        return uri;
    }

    private URI createUri() throws ElasticsearchStatusException {
        try {
            // TODO, consider transforming the base URL into a URI for better error handling.
            if (getConfigurations().getTaskType().equals(TaskType.TEXT_EMBEDDING)) {
                return getBaseURIBuilder().setPath(TEXT_EMBEDDING_PATH).build();
            } else {
                return getBaseURIBuilder().setPath(MULTIMODAL_EMBEDDING_PATH).build();
            }
        } catch (URISyntaxException e) {
            throw new ElasticsearchStatusException(
                "Failed to create URI for service ["
                    + this.getConfigurations().getService()
                    + "] with taskType ["
                    + this.getTaskType()
                    + "]: "
                    + e.getMessage(),
                RestStatus.BAD_REQUEST,
                e
            );
        }
    }
}
