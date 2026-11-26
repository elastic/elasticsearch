/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.decision;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Nullable;
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

/**
 * An Elastic Inference Service endpoint for the {@link TaskType#DECISION} task type.
 */
public class ElasticInferenceServiceDecisionModel extends ElasticInferenceServiceModel {

    public static final String DECIDE_PATH = "/api/v1/decide";
    private final URI uri;

    public ElasticInferenceServiceDecisionModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
        ConfigurationParseContext context,
        @Nullable EndpointMetadata endpointMetadata
    ) {
        this(
            inferenceEntityId,
            taskType,
            ElasticInferenceServiceDecisionServiceSettings.fromMap(serviceSettings, context),
            ElasticInferenceServiceDecisionTaskSettings.fromMap(taskSettings, context),
            elasticInferenceServiceComponents,
            endpointMetadata
        );
    }

    public ElasticInferenceServiceDecisionModel(
        String inferenceEntityId,
        TaskType taskType,
        ElasticInferenceServiceDecisionServiceSettings serviceSettings,
        ElasticInferenceServiceDecisionTaskSettings taskSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
        @Nullable EndpointMetadata endpointMetadata
    ) {
        this(
            new ModelConfigurations(
                inferenceEntityId,
                taskType,
                ElasticInferenceService.NAME,
                serviceSettings,
                taskSettings,
                null,
                endpointMetadata
            ),
            ModelSecrets.emptySecrets(),
            elasticInferenceServiceComponents
        );
    }

    public ElasticInferenceServiceDecisionModel(
        ModelConfigurations modelConfigurations,
        ModelSecrets modelSecrets,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents
    ) {
        super(
            modelConfigurations,
            modelSecrets,
            (ElasticInferenceServiceDecisionServiceSettings) modelConfigurations.getServiceSettings(),
            elasticInferenceServiceComponents
        );
        this.uri = createUri();
    }

    @Override
    public ElasticInferenceServiceDecisionServiceSettings getServiceSettings() {
        return (ElasticInferenceServiceDecisionServiceSettings) super.getServiceSettings();
    }

    @Override
    public ElasticInferenceServiceDecisionTaskSettings getTaskSettings() {
        return (ElasticInferenceServiceDecisionTaskSettings) super.getTaskSettings();
    }

    public URI uri() {
        return uri;
    }

    private URI createUri() throws ElasticsearchStatusException {
        try {
            return getBaseURIBuilder().setPath(DECIDE_PATH).build();
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
