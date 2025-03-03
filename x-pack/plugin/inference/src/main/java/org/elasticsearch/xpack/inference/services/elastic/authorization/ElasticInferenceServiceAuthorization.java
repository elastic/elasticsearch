/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.response.elastic.ElasticInferenceServiceAuthorizationResponseEntity;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is a helper class for managing the response from {@link ElasticInferenceServiceAuthorizationHandler}.
 */
public class ElasticInferenceServiceAuthorization {

    private final Map<TaskType, Set<String>> taskTypeToModels;
    private final EnumSet<TaskType> authorizedTaskTypes;
    private final Set<String> authorizedModelIds;

    /**
     * Converts an authorization response from Elastic Inference Service into the {@link ElasticInferenceServiceAuthorization} format.
     *
     * @param responseEntity the {@link ElasticInferenceServiceAuthorizationResponseEntity} response from the upstream gateway.
     * @return a new {@link ElasticInferenceServiceAuthorization}
     */
    public static ElasticInferenceServiceAuthorization of(ElasticInferenceServiceAuthorizationResponseEntity responseEntity) {
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

        return new ElasticInferenceServiceAuthorization(taskTypeToModelsMap, enabledModelsSet, enabledTaskTypesSet);
    }

    /**
     * Returns an object indicating that the cluster has no access to Elastic Inference Service.
     */
    public static ElasticInferenceServiceAuthorization newDisabledService() {
        return new ElasticInferenceServiceAuthorization(Map.of(), Set.of(), EnumSet.noneOf(TaskType.class));
    }

    private ElasticInferenceServiceAuthorization(
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
     * Returns a new {@link ElasticInferenceServiceAuthorization} object retaining only the specified task types
     * and applicable models that leverage those task types. Any task types not specified in the passed in set will be
     * excluded from the returned object. This is essentially an intersection.
     * @param taskTypes the task types to retain in the newly created object
     * @return a new object containing models and task types limited to the specified set.
     */
    public ElasticInferenceServiceAuthorization newLimitedToTaskTypes(EnumSet<TaskType> taskTypes) {
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

        Set<String> newEnabledModels = newTaskTypeToModels.values().stream().flatMap(Set::stream).collect(Collectors.toSet());

        return new ElasticInferenceServiceAuthorization(newTaskTypeToModels, newEnabledModels, taskTypesThatHaveModels);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ElasticInferenceServiceAuthorization that = (ElasticInferenceServiceAuthorization) o;
        return Objects.equals(taskTypeToModels, that.taskTypeToModels)
            && Objects.equals(authorizedTaskTypes, that.authorizedTaskTypes)
            && Objects.equals(authorizedModelIds, that.authorizedModelIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskTypeToModels, authorizedTaskTypes, authorizedModelIds);
    }
}
