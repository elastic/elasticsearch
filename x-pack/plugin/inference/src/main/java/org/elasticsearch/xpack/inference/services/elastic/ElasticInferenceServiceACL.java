/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.inference.TaskType;

import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides a structure for governing which models (if any) a cluster has access to according to the upstream Elastic Inference Service.
 * @param enabledModels a mapping of model ids to a set of {@link TaskType} to indicate which models are available and for which task types
 */
public record ElasticInferenceServiceACL(Map<String, EnumSet<TaskType>> enabledModels) {

    /**
     * Returns an object indicating that the cluster has no access to EIS.
     */
    public static ElasticInferenceServiceACL newDisabledService() {
        return new ElasticInferenceServiceACL();
    }

    public ElasticInferenceServiceACL {
        Objects.requireNonNull(enabledModels);
    }

    private ElasticInferenceServiceACL() {
        this(Map.of());
    }

    public boolean isEnabled() {
        return enabledModels.isEmpty() == false;
    }

    public EnumSet<TaskType> enabledTaskTypes() {
        return enabledModels.values().stream().flatMap(Set::stream).collect(Collectors.toCollection(() -> EnumSet.noneOf(TaskType.class)));
    }
}
