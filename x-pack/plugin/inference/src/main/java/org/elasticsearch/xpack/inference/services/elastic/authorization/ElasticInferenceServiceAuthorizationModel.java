/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Transforms the response from {@link ElasticInferenceServiceAuthorizationRequestHandler} into a format
 * for consumption by the {@link ElasticInferenceService}.
 */
public class ElasticInferenceServiceAuthorizationModel {

    private static final Logger logger = LogManager.getLogger(ElasticInferenceServiceAuthorizationModel.class);
    private static final String UNKNOWN_TASK_TYPE_LOG_MESSAGE = "Authorized endpoint id [{}] has unknown task type [{}], skipping";
    private static final String UNSUPPORTED_TASK_TYPE_LOG_MESSAGE = "Authorized endpoint id [{}] has unsupported task type [{}], skipping";

    // public because it's used in tests outside the package
    public static ElasticInferenceServiceAuthorizationModel of(
        ElasticInferenceServiceAuthorizationResponseEntity responseEntity,
        String baseEisUrl
    ) {
        var components = new ElasticInferenceServiceComponents(baseEisUrl);
        return createInternal(responseEntity.getAuthorizedEndpoints(), components);
    }

    private static ElasticInferenceServiceAuthorizationModel createInternal(
        List<ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint> responseEndpoints,
        ElasticInferenceServiceComponents components
    ) {
        var validEndpoints = new ArrayList<ElasticInferenceServiceModel>();
        for (var authorizedEndpoint : responseEndpoints) {
            var model = createModel(authorizedEndpoint, components);
            if (model != null) {
                validEndpoints.add(model);
            }
        }

        return new ElasticInferenceServiceAuthorizationModel(validEndpoints);
    }

    private static ElasticInferenceServiceModel createModel(
        ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint authorizedEndpoint,
        ElasticInferenceServiceComponents components
    ) {
        try {
            var taskType = getTaskType(authorizedEndpoint.taskType().elasticsearchTaskType());
            if (taskType == null) {
                logger.warn(UNKNOWN_TASK_TYPE_LOG_MESSAGE, authorizedEndpoint.id(), authorizedEndpoint.taskType());
                return null;
            }

            return switch (taskType) {
                case CHAT_COMPLETION -> createCompletionModel(authorizedEndpoint, TaskType.CHAT_COMPLETION, components);
                case COMPLETION -> createCompletionModel(authorizedEndpoint, TaskType.COMPLETION, components);
                case SPARSE_EMBEDDING -> createSparseTextEmbeddingsModel(authorizedEndpoint, components);
                case TEXT_EMBEDDING -> createDenseTextEmbeddingsModel(authorizedEndpoint, components);
                case RERANK -> createRerankModel(authorizedEndpoint, components);
                default -> {
                    logger.info(UNSUPPORTED_TASK_TYPE_LOG_MESSAGE, authorizedEndpoint.id(), taskType);
                    yield null;
                }
            };
        } catch (Exception e) {
            logger.atWarn()
                .withThrowable(e)
                .log(
                    "Failed to create model for authorized endpoint id [{}] with task type [{}], skipping",
                    authorizedEndpoint.id(),
                    authorizedEndpoint.taskType()
                );
            return null;
        }
    }

    private static TaskType getTaskType(String taskType) {
        try {
            return TaskType.fromString(taskType);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static ElasticInferenceServiceCompletionModel createCompletionModel(
        ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint authorizedEndpoint,
        TaskType taskType,
        ElasticInferenceServiceComponents components
    ) {
        return new ElasticInferenceServiceCompletionModel(
            authorizedEndpoint.id(),
            taskType,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceCompletionServiceSettings(authorizedEndpoint.modelName()),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            components
        );
    }

    private static ElasticInferenceServiceSparseEmbeddingsModel createSparseTextEmbeddingsModel(
        ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint authorizedEndpoint,
        ElasticInferenceServiceComponents components
    ) {
        return new ElasticInferenceServiceSparseEmbeddingsModel(
            authorizedEndpoint.id(),
            TaskType.SPARSE_EMBEDDING,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceSparseEmbeddingsServiceSettings(authorizedEndpoint.modelName(), null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            components,
            ChunkingSettingsBuilder.fromMap(getChunkingSettingsMap(getConfigurationOrEmpty(authorizedEndpoint)))
        );
    }

    private static ElasticInferenceServiceAuthorizationResponseEntity.Configuration getConfigurationOrEmpty(
        ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint authorizedEndpoint
    ) {
        if (authorizedEndpoint.configuration() != null) {
            return authorizedEndpoint.configuration();
        }

        return ElasticInferenceServiceAuthorizationResponseEntity.Configuration.EMPTY;
    }

    private static Map<String, Object> getChunkingSettingsMap(
        ElasticInferenceServiceAuthorizationResponseEntity.Configuration configuration
    ) {
        // We intentionally want to return an empty map here instead of null, because ChunkingSettingsBuilder.fromMap()
        // will return the "new" default value in that case
        return Objects.requireNonNullElse(configuration.chunkingSettings(), new HashMap<>());
    }

    private static ElasticInferenceServiceDenseTextEmbeddingsModel createDenseTextEmbeddingsModel(
        ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint authorizedEndpoint,
        ElasticInferenceServiceComponents components
    ) {
        var config = getConfigurationOrEmpty(authorizedEndpoint);
        validateConfigurationForTextEmbedding(config);

        return new ElasticInferenceServiceDenseTextEmbeddingsModel(
            authorizedEndpoint.id(),
            TaskType.TEXT_EMBEDDING,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
                authorizedEndpoint.modelName(),
                getSimilarityMeasure(config),
                config.dimensions(),
                null
            ),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            components,
            ChunkingSettingsBuilder.fromMap(getChunkingSettingsMap(config))
        );
    }

    private static void validateConfigurationForTextEmbedding(ElasticInferenceServiceAuthorizationResponseEntity.Configuration config) {
        validateFieldPresent(
            ElasticInferenceServiceAuthorizationResponseEntity.Configuration.ELEMENT_TYPE,
            config.elementType(),
            TaskType.TEXT_EMBEDDING
        );
        validateFieldPresent(
            ElasticInferenceServiceAuthorizationResponseEntity.Configuration.DIMENSIONS,
            config.dimensions(),
            TaskType.TEXT_EMBEDDING
        );
        validateFieldPresent(
            ElasticInferenceServiceAuthorizationResponseEntity.Configuration.SIMILARITY,
            config.similarity(),
            TaskType.TEXT_EMBEDDING
        );

        var configElementType = config.elementType().toLowerCase(Locale.ROOT);
        var supportedElementType = getSupportedElementType();

        if (configElementType.equals(supportedElementType) == false) {
            throw new IllegalArgumentException(
                Strings.format("Unsupported element type encountered [%s], only [%s] is supported", configElementType, supportedElementType)
            );
        }
    }

    private static String getSupportedElementType() {
        return ElasticInferenceServiceDenseTextEmbeddingsServiceSettings.SUPPORTED_ELEMENT_TYPE.toString().toLowerCase(Locale.ROOT);
    }

    private static void validateFieldPresent(String field, Object fieldValue, TaskType taskType) {
        if (fieldValue == null) {
            throw new IllegalArgumentException(
                Strings.format("Required field [%s] is missing for task_type [%s]", field, taskType.toString())
            );
        }
    }

    private static SimilarityMeasure getSimilarityMeasure(ElasticInferenceServiceAuthorizationResponseEntity.Configuration configuration) {
        return SimilarityMeasure.fromString(configuration.similarity());
    }

    private static ElasticInferenceServiceRerankModel createRerankModel(
        ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedEndpoint authorizedEndpoint,
        ElasticInferenceServiceComponents components
    ) {
        return new ElasticInferenceServiceRerankModel(
            authorizedEndpoint.id(),
            TaskType.RERANK,
            ElasticInferenceService.NAME,
            new ElasticInferenceServiceRerankServiceSettings(authorizedEndpoint.modelName()),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            components
        );
    }

    /**
     * Returns an object indicating that the cluster is not authorized for any endpoints from EIS.
     */
    public static ElasticInferenceServiceAuthorizationModel unauthorized() {
        return new ElasticInferenceServiceAuthorizationModel(List.of());
    }

    private final Map<String, ElasticInferenceServiceModel> authorizedEndpoints;
    private final EnumSet<TaskType> taskTypes;

    // Default for testing
    ElasticInferenceServiceAuthorizationModel(List<ElasticInferenceServiceModel> authorizedEndpoints) {
        Objects.requireNonNull(authorizedEndpoints);
        this.authorizedEndpoints = authorizedEndpoints.stream()
            .collect(
                Collectors.toMap(ElasticInferenceServiceModel::getInferenceEntityId, Function.identity(), (firstModel, secondModel) -> {
                    logger.warn("Found inference id collision for id [{}], ignoring second model", firstModel.inferenceEntityId());
                    return firstModel;
                }, HashMap::new)
            );

        var taskTypesSet = EnumSet.noneOf(TaskType.class);
        taskTypesSet.addAll(this.authorizedEndpoints.values().stream().map(ElasticInferenceServiceModel::getTaskType).toList());
        this.taskTypes = taskTypesSet;
    }

    /**
     * Returns true if at least one endpoint is authorized.
     * @return true if this cluster is authorized for at least one endpoint.
     */
    public boolean isAuthorized() {
        return authorizedEndpoints.isEmpty() == false;
    }

    /**
     * Returns a new {@link ElasticInferenceServiceAuthorizationModel} object retaining only the specified task types
     * and applicable models that leverage those task types. Any task types not specified in the provided parameter will be
     * excluded from the returned object. This is essentially an intersection.
     * @param taskTypes the task types to retain in the newly created object
     * @return a new object containing endpoints limited to the specified task types
     */
    public ElasticInferenceServiceAuthorizationModel newLimitedToTaskTypes(EnumSet<TaskType> taskTypes) {
        var endpoints = this.authorizedEndpoints.values().stream().filter(endpoint -> taskTypes.contains(endpoint.getTaskType())).toList();
        return new ElasticInferenceServiceAuthorizationModel(endpoints);
    }

    public EnumSet<TaskType> getTaskTypes() {
        return EnumSet.copyOf(taskTypes);
    }

    public Set<String> getEndpointIds() {
        return Set.copyOf(authorizedEndpoints.keySet());
    }

    public List<Model> getEndpoints(Set<String> endpointIds) {
        return endpointIds.stream().<Model>map(authorizedEndpoints::get).filter(Objects::nonNull).toList();
    }

    @Override
    public String toString() {
        return Strings.format("AuthorizationModel{authorizedEndpoints=%s, taskTypes=%s}", authorizedEndpoints, taskTypes);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ElasticInferenceServiceAuthorizationModel that = (ElasticInferenceServiceAuthorizationModel) o;
        return Objects.equals(authorizedEndpoints, that.authorizedEndpoints) && Objects.equals(taskTypes, that.taskTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authorizedEndpoints, taskTypes);
    }
}
