/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transforms the response from {@link ElasticInferenceServiceAuthorizationRequestHandler} into a format for consumption by the service.
 */
public class ElasticInferenceServiceAuthorizationModel {

    private final Map<TaskType, Set<String>> taskTypeToModels;
    private final EnumSet<TaskType> authorizedTaskTypes;
    private final Set<String> authorizedModelIds;

    /**
     * Converts an authorization response from Elastic Inference Service into the {@link ElasticInferenceServiceAuthorizationModel} format.
     *
     * @param responseEntity the {@link ElasticInferenceServiceAuthorizationResponseEntity} response from the upstream gateway.
     * @return a new {@link ElasticInferenceServiceAuthorizationModel}
     */
    public static ElasticInferenceServiceAuthorizationModel of(ElasticInferenceServiceAuthorizationResponseEntity responseEntity) {
        var taskTypeToModelsMap = new HashMap<TaskType, Set<String>>();
        var enabledTaskTypesSet = EnumSet.noneOf(TaskType.class);
        var enabledModelsSet = new HashSet<String>();

        for (var model : responseEntity.getAuthorizedModels()) {
            // if there are no task types we'll ignore the model because it's likely we didn't understand
            // the task type and don't support it anyway
            if (model.taskTypes().isEmpty() == false) {
                for (var taskType : model.taskTypes()) {
                    taskTypeToModelsMap.merge(taskType, Set.of(model.modelName()), (existingModelIds, newModelIds) -> {
                        var combinedNames = new HashSet<>(existingModelIds);
                        combinedNames.addAll(newModelIds);
                        return combinedNames;
                    });
                    enabledTaskTypesSet.add(taskType);
                }
                enabledModelsSet.add(model.modelName());
            }
        }

        return new ElasticInferenceServiceAuthorizationModel(taskTypeToModelsMap, enabledModelsSet, enabledTaskTypesSet);
    }

    /**
     * Returns an object indicating that the cluster has no access to Elastic Inference Service.
     */
    public static ElasticInferenceServiceAuthorizationModel newDisabledService() {
        return new ElasticInferenceServiceAuthorizationModel(Map.of(), Set.of(), EnumSet.noneOf(TaskType.class));
    }

    private ElasticInferenceServiceAuthorizationModel(
        Map<TaskType, Set<String>> taskTypeToModels,
        Set<String> authorizedModelIds,
        EnumSet<TaskType> authorizedTaskTypes
    ) {
        this.taskTypeToModels = Objects.requireNonNull(taskTypeToModels);
        this.authorizedModelIds = Objects.requireNonNull(authorizedModelIds);
        this.authorizedTaskTypes = Objects.requireNonNull(authorizedTaskTypes);
    }

    /**
     * Returns true if at least one task type and model is authorized.
     * @return true if this cluster is authorized for at least one model and task type.
     */
    public boolean isAuthorized() {
        return authorizedModelIds.isEmpty() == false && taskTypeToModels.isEmpty() == false && authorizedTaskTypes.isEmpty() == false;
    }

    public Set<String> getAuthorizedModelIds() {
        return Set.copyOf(authorizedModelIds);
    }

    public EnumSet<TaskType> getAuthorizedTaskTypes() {
        return EnumSet.copyOf(authorizedTaskTypes);
    }

    /**
     * Returns a new {@link ElasticInferenceServiceAuthorizationModel} object retaining only the specified task types
     * and applicable models that leverage those task types. Any task types not specified in the passed in set will be
     * excluded from the returned object. This is essentially an intersection.
     * @param taskTypes the task types to retain in the newly created object
     * @return a new object containing models and task types limited to the specified set.
     */
    public ElasticInferenceServiceAuthorizationModel newLimitedToTaskTypes(EnumSet<TaskType> taskTypes) {
        var newTaskTypeToModels = new HashMap<TaskType, Set<String>>();
        var taskTypesThatHaveModels = EnumSet.noneOf(TaskType.class);

        for (var taskType : taskTypes) {
            var models = taskTypeToModels.get(taskType);
            if (models != null) {
                newTaskTypeToModels.put(taskType, models);
                // we only want task types that correspond to actual models to ensure we're only enabling valid task types
                taskTypesThatHaveModels.add(taskType);
            }
        }

        return new ElasticInferenceServiceAuthorizationModel(
            newTaskTypeToModels,
            enabledModels(newTaskTypeToModels),
            taskTypesThatHaveModels
        );
    }

    private static Set<String> enabledModels(Map<TaskType, Set<String>> taskTypeToModels) {
        return taskTypeToModels.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    }

    /**
     * Returns a new {@link ElasticInferenceServiceAuthorizationModel} that combines the current model and the passed in one.
     * @param other model to merge into this one
     * @return a new model
     */
    public ElasticInferenceServiceAuthorizationModel merge(ElasticInferenceServiceAuthorizationModel other) {
        Map<TaskType, Set<String>> newTaskTypeToModels = taskTypeToModels.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>(e.getValue())));

        for (var entry : other.taskTypeToModels.entrySet()) {
            newTaskTypeToModels.merge(entry.getKey(), new HashSet<>(entry.getValue()), (existingModelIds, newModelIds) -> {
                existingModelIds.addAll(newModelIds);
                return existingModelIds;
            });
        }

        var newAuthorizedTaskTypes = authorizedTaskTypes.isEmpty() ? EnumSet.noneOf(TaskType.class) : EnumSet.copyOf(authorizedTaskTypes);
        newAuthorizedTaskTypes.addAll(other.authorizedTaskTypes);

        return new ElasticInferenceServiceAuthorizationModel(
            newTaskTypeToModels,
            enabledModels(newTaskTypeToModels),
            newAuthorizedTaskTypes
        );
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ElasticInferenceServiceAuthorizationModel that = (ElasticInferenceServiceAuthorizationModel) o;
        return Objects.equals(taskTypeToModels, that.taskTypeToModels)
            && Objects.equals(authorizedTaskTypes, that.authorizedTaskTypes)
            && Objects.equals(authorizedModelIds, that.authorizedModelIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskTypeToModels, authorizedTaskTypes, authorizedModelIds);
    }
}
