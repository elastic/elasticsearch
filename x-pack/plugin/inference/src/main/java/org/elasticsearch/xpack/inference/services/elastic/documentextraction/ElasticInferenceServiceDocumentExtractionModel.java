/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

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

public class ElasticInferenceServiceDocumentExtractionModel extends ElasticInferenceServiceModel {

    public static final String DOCUMENT_EXTRACTION_PATH = "/api/v1/document-extraction";

    /**
     * Creates a copy of {@code model} carrying {@code taskSettings} instead of the stored ones. Document extraction accepts task
     * settings in the inference request body, so this is used for the lifetime of a single inference call to apply the request's
     * settings on top of the stored ones without touching the persisted endpoint.
     */
    public static ElasticInferenceServiceDocumentExtractionModel of(
        ElasticInferenceServiceDocumentExtractionModel model,
        ElasticInferenceServiceDocumentExtractionTaskSettings taskSettings
    ) {
        return new ElasticInferenceServiceDocumentExtractionModel(model, taskSettings);
    }

    private final URI uri;

    public ElasticInferenceServiceDocumentExtractionModel(
        ElasticInferenceServiceDocumentExtractionModel model,
        ElasticInferenceServiceDocumentExtractionTaskSettings taskSettings
    ) {
        super(model, taskSettings);
        this.uri = model.uri();
    }

    public ElasticInferenceServiceDocumentExtractionModel(
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
            ElasticInferenceServiceDocumentExtractionServiceSettings.fromMap(serviceSettings, context),
            ElasticInferenceServiceDocumentExtractionTaskSettings.fromMap(taskSettings),
            elasticInferenceServiceComponents,
            endpointMetadata
        );
    }

    public ElasticInferenceServiceDocumentExtractionModel(
        String inferenceEntityId,
        TaskType taskType,
        ElasticInferenceServiceDocumentExtractionServiceSettings serviceSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents
    ) {
        this(inferenceEntityId, taskType, serviceSettings, elasticInferenceServiceComponents, null);
    }

    public ElasticInferenceServiceDocumentExtractionModel(
        String inferenceEntityId,
        TaskType taskType,
        ElasticInferenceServiceDocumentExtractionServiceSettings serviceSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents,
        @Nullable EndpointMetadata endpointMetadata
    ) {
        this(
            inferenceEntityId,
            taskType,
            serviceSettings,
            ElasticInferenceServiceDocumentExtractionTaskSettings.EMPTY_SETTINGS,
            elasticInferenceServiceComponents,
            endpointMetadata
        );
    }

    public ElasticInferenceServiceDocumentExtractionModel(
        String inferenceEntityId,
        TaskType taskType,
        ElasticInferenceServiceDocumentExtractionServiceSettings serviceSettings,
        ElasticInferenceServiceDocumentExtractionTaskSettings taskSettings,
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

    public ElasticInferenceServiceDocumentExtractionModel(
        ModelConfigurations modelConfigurations,
        ModelSecrets modelSecrets,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents
    ) {
        super(
            modelConfigurations,
            modelSecrets,
            (ElasticInferenceServiceDocumentExtractionServiceSettings) modelConfigurations.getServiceSettings(),
            elasticInferenceServiceComponents
        );
        this.uri = createUri();
    }

    @Override
    public ElasticInferenceServiceDocumentExtractionServiceSettings getServiceSettings() {
        return (ElasticInferenceServiceDocumentExtractionServiceSettings) super.getServiceSettings();
    }

    @Override
    public ElasticInferenceServiceDocumentExtractionTaskSettings getTaskSettings() {
        return (ElasticInferenceServiceDocumentExtractionTaskSettings) super.getTaskSettings();
    }

    public URI uri() {
        return uri;
    }

    private URI createUri() throws ElasticsearchStatusException {
        try {
            return getBaseURIBuilder().setPath(DOCUMENT_EXTRACTION_PATH).build();
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
