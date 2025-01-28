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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides a structure for governing which models (if any) a cluster has access to according to the upstream Elastic Inference Service.
 * @param enabledModels a mapping of model ids to a set of {@link TaskType} to indicate which models are available and for which task types
 */
public record ElasticInferenceServiceAuthorization(Map<String, EnumSet<TaskType>> enabledModels) {

    /**
     * Converts an authorization response from Elastic Inference Service into the {@link ElasticInferenceServiceAuthorization} format.
     *
     * @param responseEntity the {@link ElasticInferenceServiceAuthorizationResponseEntity} response from the upstream gateway.
     * @return a new {@link ElasticInferenceServiceAuthorization}
     */
    public static ElasticInferenceServiceAuthorization of(ElasticInferenceServiceAuthorizationResponseEntity responseEntity) {
        var enabledModels = new HashMap<String, EnumSet<TaskType>>();

        for (var model : responseEntity.getAuthorizedModels()) {
            // if there are no task types we'll ignore the model because it's likely we didn't understand
            // the task type and don't support it anyway
            if (model.taskTypes().isEmpty() == false) {
                enabledModels.put(model.modelName(), model.taskTypes());
            }
        }

        return new ElasticInferenceServiceAuthorization(enabledModels);
    }

    /**
     * Returns an object indicating that the cluster has no access to Elastic Inference Service.
     */
    public static ElasticInferenceServiceAuthorization newDisabledService() {
        return new ElasticInferenceServiceAuthorization();
    }

    public ElasticInferenceServiceAuthorization {
        Objects.requireNonNull(enabledModels);

        for (var taskTypes : enabledModels.values()) {
            if (taskTypes.isEmpty()) {
                throw new IllegalArgumentException("Authorization task types must not be empty");
            }
        }
    }

    private ElasticInferenceServiceAuthorization() {
        this(Map.of());
    }

    public boolean isEnabled() {
        return enabledModels.isEmpty() == false;
    }

    public EnumSet<TaskType> enabledTaskTypes() {
        return enabledModels.values().stream().flatMap(Set::stream).collect(Collectors.toCollection(() -> EnumSet.noneOf(TaskType.class)));
    }
}
