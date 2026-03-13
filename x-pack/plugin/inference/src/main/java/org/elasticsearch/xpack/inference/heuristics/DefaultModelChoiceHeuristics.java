/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.heuristics;

import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Central registry of model choice heuristics for Elasticsearch features. Each feature that needs a default
 * inference endpoint defines its heuristic here using the {@link ModelChoiceHeuristics} builder DSL.
 * <p>
 * This keeps all heuristic definitions in one place, separate from the feature code that uses them.
 */
public final class DefaultModelChoiceHeuristics {

    /**
     * Heuristic for {@code semantic_text} fields.
     * <p>
     * Priority 1: multilingual dense (text_embedding) model, newest release date wins.
     * Priority 2: english sparse (sparse_embedding) model, newest release date wins.
     */
    static final ModelChoiceHeuristics SEMANTIC_TEXT = ModelChoiceHeuristics.forFeature("semantic_text")
        .priority(1)
        .filterByTaskType(TaskType.TEXT_EMBEDDING)
        .filterByProperty("multilingual")
        .filterByNotDeprecated()
        .filterByNotEndOfLife()
        .orderByReleaseDateDesc()
        .priority(2)
        .filterByTaskType(TaskType.SPARSE_EMBEDDING)
        .filterByProperty("english")
        .filterByNotDeprecated()
        .filterByNotEndOfLife()
        .orderByReleaseDateDesc()
        .build();

    private DefaultModelChoiceHeuristics() {}

    /**
     * Selects the best default inference endpoint for {@code semantic_text} fields using the registered
     * model heuristics. Queries the model registry for all available endpoints and applies the
     * semantic_text heuristic to choose the best one.
     *
     * @param modelRegistry the model registry to query for available endpoints
     * @return the selected inference endpoint ID, or empty if no suitable endpoint is found
     */
    public static Optional<String> selectSemanticTextEndpoint(ModelRegistry modelRegistry) {
        Set<String> allIds = modelRegistry.getInferenceIds();
        if (allIds.isEmpty()) {
            return Optional.empty();
        }

        Map<String, MinimalServiceSettings> candidates = modelRegistry.getMinimalServiceSettings(allIds, false);
        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        return SEMANTIC_TEXT.selectBestEndpoint(candidates);
    }
}
