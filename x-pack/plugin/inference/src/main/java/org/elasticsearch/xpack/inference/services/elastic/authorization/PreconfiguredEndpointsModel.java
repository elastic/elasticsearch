/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceMinimalSettings;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the preconfigured endpoints available from the Elastic Inference Service based on the authorization information retrieved.
 */
public record PreconfiguredEndpointsModel(Map<String, PreconfiguredEndpoint> preconfiguredEndpoints) {
    private static final Logger logger = LogManager.getLogger(PreconfiguredEndpointsModel.class);

    public static PreconfiguredEndpointsModel of(ElasticInferenceServiceAuthorizationModel authModel) {
        var endpoints = authModel.getAuthorizedModelIds()
            .stream()
            .filter(ElasticInferenceServiceMinimalSettings::containsModelName)
            .map((modelId) -> of(ElasticInferenceServiceMinimalSettings.getWithModelName(modelId)))
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(PreconfiguredEndpoint::inferenceEntityId, Function.identity()));

        return new PreconfiguredEndpointsModel(endpoints);
    }

    private static PreconfiguredEndpoint of(ElasticInferenceServiceMinimalSettings.SettingsWithEndpointInfo settings) {
        return switch (settings.minimalSettings().taskType()) {
            case TEXT_EMBEDDING -> {
                if (settings.minimalSettings().dimensions() == null
                    || settings.minimalSettings().similarity() == null
                    || settings.minimalSettings().elementType() == null) {
                    logger.warn(
                        "Skipping embedding endpoint [{}] as it is missing required settings. "
                            + "Dimensions: [{}], Similarity: [{}], Element Type: [{}]",
                        settings.inferenceId(),
                        settings.minimalSettings().dimensions(),
                        settings.minimalSettings().similarity(),
                        settings.minimalSettings().elementType()
                    );
                    yield null;
                }

                yield new EmbeddingPreConfiguredEndpoint(
                    settings.inferenceId(),
                    settings.minimalSettings().taskType(),
                    settings.modelId(),
                    settings.minimalSettings().similarity(),
                    settings.minimalSettings().dimensions(),
                    settings.minimalSettings().elementType()
                );
            }
            case SPARSE_EMBEDDING, RERANK, COMPLETION, CHAT_COMPLETION -> new BasePreconfiguredEndpoint(
                settings.inferenceId(),
                settings.minimalSettings().taskType(),
                settings.modelId()
            );
            case ANY -> null;
        };
    }

    public sealed interface PreconfiguredEndpoint permits BasePreconfiguredEndpoint, EmbeddingPreConfiguredEndpoint {
        String inferenceEntityId();

        TaskType taskType();

        String modelId();

        UnparsedModel toUnparsedModel();
    }

    private record EmbeddingPreConfiguredEndpoint(
        String inferenceEntityId,
        TaskType taskType,
        String modelId,
        SimilarityMeasure similarity,
        int dimension,
        DenseVectorFieldMapper.ElementType elementType
    ) implements PreconfiguredEndpoint {

        @Override
        public UnparsedModel toUnparsedModel() {
            return new UnparsedModel(
                inferenceEntityId,
                taskType,
                ElasticInferenceService.NAME,
                embeddingSettings(modelId, similarity, dimension, elementType),
                Map.of()
            );
        }
    }

    private static Map<String, Object> embeddingSettings(
        String modelId,
        SimilarityMeasure similarityMeasure,
        int dimension,
        DenseVectorFieldMapper.ElementType elementType
    ) {
        return wrapWithServiceSettings(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    ServiceFields.SIMILARITY,
                    similarityMeasure.toString(),
                    ServiceFields.DIMENSIONS,
                    dimension,
                    ServiceFields.ELEMENT_TYPE,
                    elementType.toString()
                )
            )
        );
    }

    private static Map<String, Object> wrapWithServiceSettings(Map<String, Object> settings) {
        return new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, settings));
    }

    private record BasePreconfiguredEndpoint(String inferenceEntityId, TaskType taskType, String modelId) implements PreconfiguredEndpoint {
        @Override
        public UnparsedModel toUnparsedModel() {
            return new UnparsedModel(inferenceEntityId, taskType, ElasticInferenceService.NAME, settingsWithModelId(modelId), Map.of());
        }
    }

    private static Map<String, Object> settingsWithModelId(String modelId) {
        return wrapWithServiceSettings(new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)));
    }

    public UnparsedModel toUnparsedModel(String inferenceId) {
        PreconfiguredEndpoint endpoint = preconfiguredEndpoints.get(inferenceId);
        if (endpoint == null) {
            throw new IllegalArgumentException("No EIS preconfigured endpoint found for inference ID: " + inferenceId);
        }

        return endpoint.toUnparsedModel();
    }

    public List<UnparsedModel> toUnparsedModels() {
        return preconfiguredEndpoints.values()
            .stream()
            .map(PreconfiguredEndpoint::toUnparsedModel)
            .sorted(Comparator.comparing(UnparsedModel::inferenceEntityId))
            .toList();
    }
}
