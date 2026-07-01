/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.heuristics;

import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.metadata.EndpointMetadata;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * A fluent builder DSL for defining model choice heuristics. Each heuristic is a prioritized list of
 * filter-and-order rules evaluated against the available inference endpoints. The first priority level
 * that matches at least one endpoint wins; within that level, endpoints are ordered by the specified
 * comparators and the best one is returned.
 *
 * <p>Example usage:
 * <pre>{@code
 * ModelChoiceHeuristics heuristic = ModelChoiceHeuristics.forFeature("semantic_text")
 *     .priority(1)
 *         .filterByTaskType(TaskType.TEXT_EMBEDDING)
 *         .filterByProperty("multilingual")
 *         .orderByReleaseDateDesc()
 *     .priority(2)
 *         .filterByTaskType(TaskType.SPARSE_EMBEDDING)
 *         .filterByProperty("english")
 *         .orderByReleaseDateDesc()
 *     .build();
 * }</pre>
 */
public class ModelChoiceHeuristics {

    private final String featureName;
    private final List<PriorityLevel> priorities;

    private ModelChoiceHeuristics(String featureName, List<PriorityLevel> priorities) {
        this.featureName = featureName;
        this.priorities = priorities;
    }

    public String featureName() {
        return featureName;
    }

    /**
     * Selects the best inference endpoint ID from the given candidates by evaluating priority levels in order.
     * Returns the first match from the highest-priority level that has candidates.
     *
     * @param candidates a map of inference endpoint ID to its {@link MinimalServiceSettings}
     * @return the selected endpoint ID, or empty if no candidate matches any priority level
     */
    public Optional<String> selectBestEndpoint(Map<String, MinimalServiceSettings> candidates) {
        for (PriorityLevel priority : priorities) {
            Optional<String> result = priority.selectBest(candidates);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    public static Builder forFeature(String featureName) {
        return new Builder(featureName);
    }

    /**
     * A single priority level consisting of filters (all must match) and an ordering (to pick the best among matches).
     */
    public static class PriorityLevel {
        private final int priority;
        private final List<Predicate<Map.Entry<String, MinimalServiceSettings>>> filters;
        private final Comparator<Map.Entry<String, MinimalServiceSettings>> ordering;

        PriorityLevel(
            int priority,
            List<Predicate<Map.Entry<String, MinimalServiceSettings>>> filters,
            Comparator<Map.Entry<String, MinimalServiceSettings>> ordering
        ) {
            this.priority = priority;
            this.filters = filters;
            this.ordering = ordering;
        }

        public int priority() {
            return priority;
        }

        Optional<String> selectBest(Map<String, MinimalServiceSettings> candidates) {
            return candidates.entrySet()
                .stream()
                .filter(entry -> filters.stream().allMatch(f -> f.test(entry)))
                .min(ordering)
                .map(Map.Entry::getKey);
        }
    }

    public static class Builder {
        private final String featureName;
        private final List<PriorityLevelBuilder> priorityBuilders = new ArrayList<>();

        Builder(String featureName) {
            this.featureName = featureName;
        }

        public PriorityLevelBuilder priority(int level) {
            var priorityBuilder = new PriorityLevelBuilder(this, level);
            priorityBuilders.add(priorityBuilder);
            return priorityBuilder;
        }

        public ModelChoiceHeuristics build() {
            if (priorityBuilders.isEmpty()) {
                throw new IllegalStateException("At least one priority level must be defined for feature [" + featureName + "]");
            }
            List<PriorityLevel> levels = new ArrayList<>();
            for (PriorityLevelBuilder pb : priorityBuilders) {
                levels.add(pb.buildLevel());
            }
            levels.sort(Comparator.comparingInt(PriorityLevel::priority));
            return new ModelChoiceHeuristics(featureName, levels);
        }
    }

    public static class PriorityLevelBuilder {
        private final Builder parent;
        private final int level;
        private final List<Predicate<Map.Entry<String, MinimalServiceSettings>>> filters = new ArrayList<>();
        private Comparator<Map.Entry<String, MinimalServiceSettings>> ordering = Comparator.comparing(Map.Entry::getKey);

        PriorityLevelBuilder(Builder parent, int level) {
            this.parent = parent;
            this.level = level;
        }

        public PriorityLevelBuilder filterByTaskType(TaskType taskType) {
            filters.add(entry -> entry.getValue().taskType() == taskType);
            return this;
        }

        public PriorityLevelBuilder filterByProperty(String property) {
            filters.add(entry -> {
                EndpointMetadata metadata = entry.getValue().endpointMetadata();
                if (metadata == null || metadata.heuristics() == null) {
                    return false;
                }
                return metadata.heuristics().properties().contains(property);
            });
            return this;
        }

        public PriorityLevelBuilder filterByStatus(StatusHeuristic status) {
            filters.add(entry -> {
                EndpointMetadata metadata = entry.getValue().endpointMetadata();
                if (metadata == null || metadata.heuristics() == null) {
                    return false;
                }
                return metadata.heuristics().status() == status;
            });
            return this;
        }

        public PriorityLevelBuilder filterByNotDeprecated() {
            filters.add(entry -> {
                EndpointMetadata metadata = entry.getValue().endpointMetadata();
                if (metadata == null || metadata.heuristics() == null) {
                    return true;
                }
                return metadata.heuristics().status() != StatusHeuristic.DEPRECATED;
            });
            return this;
        }

        public PriorityLevelBuilder filterByNotEndOfLife() {
            filters.add(entry -> {
                EndpointMetadata metadata = entry.getValue().endpointMetadata();
                if (metadata == null || metadata.heuristics() == null) {
                    return true;
                }
                LocalDate eol = metadata.heuristics().endOfLifeDate();
                return eol == null || eol.isAfter(LocalDate.now());
            });
            return this;
        }

        /**
         * Orders candidates by release date descending (newest first).
         * Endpoints without a release date are sorted last.
         */
        public PriorityLevelBuilder orderByReleaseDateDesc() {
            this.ordering = Comparator.<Map.Entry<String, MinimalServiceSettings>, LocalDate>comparing(entry -> {
                EndpointMetadata metadata = entry.getValue().endpointMetadata();
                if (metadata == null || metadata.heuristics() == null || metadata.heuristics().releaseDate() == null) {
                    return LocalDate.MIN;
                }
                return metadata.heuristics().releaseDate();
            }).reversed().thenComparing(Map.Entry::getKey);
            return this;
        }

        /**
         * Chains to the next priority level.
         */
        public PriorityLevelBuilder priority(int nextLevel) {
            return parent.priority(nextLevel);
        }

        public ModelChoiceHeuristics build() {
            return parent.build();
        }

        PriorityLevel buildLevel() {
            return new PriorityLevel(level, filters, ordering);
        }
    }
}
