/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.telemetry;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public record InferenceStats(
    LongCounter requestCount,
    LongHistogram inferenceDuration,
    LongHistogram deploymentDuration,
    Map<String, Object> constantAttributes
) {

    static final String STACK_VERSION_ATTRIBUTE = "stack_version";
    static final String IS_PRODUCTION_RELEASE_ATTRIBUTE = "is_production_release";
    static final String SERVICE_ATTRIBUTE = "service";
    static final String TASK_TYPE_ATTRIBUTE = "task_type";
    static final String STATUS_CODE_ATTRIBUTE = "status_code";
    static final String ERROR_TYPE_ATTRIBUTE = "error_type";

    public InferenceStats {
        Objects.requireNonNull(requestCount);
        Objects.requireNonNull(inferenceDuration);
        Objects.requireNonNull(deploymentDuration);
        Objects.requireNonNull(constantAttributes);
    }

    public static InferenceStats create(MeterRegistry meterRegistry, String stackVersion, boolean isProductionRelease) {
        return new InferenceStats(
            meterRegistry.registerLongCounter(
                "es.inference.requests.count.total",
                "Inference API request counts for a particular service, task type, model ID",
                "operations"
            ),
            meterRegistry.registerLongHistogram(
                "es.inference.requests.time",
                "Inference API request counts for a particular service, task type, model ID",
                "ms"
            ),
            meterRegistry.registerLongHistogram(
                "es.inference.trained_model.deployment.time",
                "Inference API time spent waiting for Trained Model Deployments",
                "ms"
            ),
            Map.of(STACK_VERSION_ATTRIBUTE, stackVersion, IS_PRODUCTION_RELEASE_ATTRIBUTE, isProductionRelease)
        );
    }

    /**
     * Merges the cluster-level constant attributes (e.g. stack version, production release flag)
     * into the provided per-request attribute map and returns the combined map.
     * Use this only when no {@link Model} is available (e.g. model-not-found errors).
     * Prefer {@link #serviceAttributes} or {@link #serviceAndResponseAttributes} otherwise.
     */
    public Map<String, Object> withConstantAttributes(Map<String, Object> attributes) {
        var result = new HashMap<>(attributes);
        result.putAll(constantAttributes);
        return result;
    }

    public Map<String, Object> serviceAttributes(Model model) {
        var result = new HashMap<String, Object>();
        result.put(SERVICE_ATTRIBUTE, model.getConfigurations().getService());
        result.put(TASK_TYPE_ATTRIBUTE, model.getTaskType().toString());
        result.putAll(constantAttributes);
        return result;
    }

    public static Map<String, Object> responseAttributes(@Nullable Throwable throwable) {
        if (Objects.isNull(throwable)) {
            return Map.of(STATUS_CODE_ATTRIBUTE, 200);
        }

        if (throwable instanceof ElasticsearchStatusException ese) {
            return Map.of(STATUS_CODE_ATTRIBUTE, ese.status().getStatus(), ERROR_TYPE_ATTRIBUTE, String.valueOf(ese.status().getStatus()));
        }

        return Map.of(ERROR_TYPE_ATTRIBUTE, throwable.getClass().getSimpleName());
    }

    public Map<String, Object> serviceAndResponseAttributes(Model model, @Nullable Throwable throwable) {
        var result = serviceAttributes(model);
        result.putAll(responseAttributes(throwable));
        return result;
    }
}
