/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.rerank;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Nullable;
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

public class ElasticInferenceServiceRerankModel extends ElasticInferenceServiceModel {

    public static final String RERANK_PATH = "/api/v1/rerank/text/text-similarity";
    private final URI uri;

    public ElasticInferenceServiceRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
        ConfigurationParseContext context,
        @Nullable EndpointMetadata endpointMetadata
    ) {
        this(
            inferenceEntityId,
            taskType,
            ElasticInferenceServiceRerankServiceSettings.fromMap(serviceSettings, context),
            elasticInferenceServiceComponents,
            endpointMetadata
        );
    }

    public ElasticInferenceServiceRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        ElasticInferenceServiceRerankServiceSettings serviceSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents
    ) {
        this(inferenceEntityId, taskType, serviceSettings, elasticInferenceServiceComponents, null);
    }

    public ElasticInferenceServiceRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        ElasticInferenceServiceRerankServiceSettings serviceSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
        @Nullable EndpointMetadata endpointMetadata
    ) {
        this(
            new ModelConfigurations(
                inferenceEntityId,
                taskType,
                ElasticInferenceService.NAME,
                serviceSettings,
                EmptyTaskSettings.INSTANCE,
                null,
                endpointMetadata
            ),
            ModelSecrets.emptySecrets(),
            elasticInferenceServiceComponents
        );
    }

    public ElasticInferenceServiceRerankModel(
        ModelConfigurations modelConfigurations,
        ModelSecrets modelSecrets,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents
    ) {
        super(
            modelConfigurations,
            modelSecrets,
            (ElasticInferenceServiceRerankServiceSettings) modelConfigurations.getServiceSettings(),
            elasticInferenceServiceComponents
        );
        this.uri = createUri();
    }

    @Override
    public ElasticInferenceServiceRerankServiceSettings getServiceSettings() {
        return (ElasticInferenceServiceRerankServiceSettings) super.getServiceSettings();
    }

    public URI uri() {
        return uri;
    }

    private URI createUri() throws ElasticsearchStatusException {
        try {
            // TODO, consider transforming the base URL into a URI for better error handling.
            return getBaseURIBuilder().setPath(RERANK_PATH).build();
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
