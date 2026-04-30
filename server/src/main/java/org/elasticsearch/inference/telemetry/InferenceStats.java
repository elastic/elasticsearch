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

import static org.elasticsearch.telemetry.metric.MetricAttributes.ERROR_TYPE;

public record InferenceStats(
    LongCounter requestCount,
    LongHistogram inferenceDuration,
    LongHistogram deploymentDuration,
    Map<String, Object> constantAttributes
) {

    // These attributes predated the "es_" prefix requirement
    public static final String SERVICE_ATTRIBUTE = "service";
    public static final String TASK_TYPE_ATTRIBUTE = "task_type";
    public static final String STATUS_CODE_ATTRIBUTE = "status_code";
    public static final String INFERENCE_SOURCE_ATTRIBUTE = "inference_source";

    public static final String INFERENCE_REQUEST_COUNT_TOTAL = "es.inference.requests.count.total";
    public static final String INFERENCE_REQUEST_DURATION = "es.inference.requests.time";
    public static final String INFERENCE_DEPLOYMENT_DURATION = "es.inference.trained_model.deployment.time";

    // Attribute keys must start with "es_" prefix see org.elasticsearch.telemetry.apm.internal.MetricValidator#validateAttributeKey
    static final String STACK_VERSION_ATTRIBUTE = "es_stack_version";
    // Indicates whether the node is a production release (i.e. not a snapshot, alpha, etc)
    static final String PRODUCTION_RELEASE_ATTRIBUTE = "es_production_release";

    public InferenceStats {
        Objects.requireNonNull(requestCount);
        Objects.requireNonNull(inferenceDuration);
        Objects.requireNonNull(deploymentDuration);
        Objects.requireNonNull(constantAttributes);
    }

    public static InferenceStats create(MeterRegistry meterRegistry, String stackVersion, boolean isProductionRelease) {
        return new InferenceStats(
            meterRegistry.registerLongCounter(
                INFERENCE_REQUEST_COUNT_TOTAL,
                "Inference API request counts for a particular service and task type",
                "operations"
            ),
            meterRegistry.registerLongHistogram(
                INFERENCE_REQUEST_DURATION,
                "Inference API request counts for a particular service and task type",
                "ms"
            ),
            meterRegistry.registerLongHistogram(
                INFERENCE_DEPLOYMENT_DURATION,
                "Inference API time spent waiting for Trained Model Deployments",
                "ms"
            ),
            Map.of(STACK_VERSION_ATTRIBUTE, stackVersion, PRODUCTION_RELEASE_ATTRIBUTE, isProductionRelease)
        );
    }

    /**
     * Merges the node-level constant attributes (e.g. stack version, production release flag)
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
            return Map.of(STATUS_CODE_ATTRIBUTE, ese.status().getStatus(), ERROR_TYPE, String.valueOf(ese.status().getStatus()));
        }

        return Map.of(ERROR_TYPE, throwable.getClass().getSimpleName());
    }

    public Map<String, Object> serviceAndResponseAttributes(Model model, @Nullable Throwable throwable) {
        var result = serviceAttributes(model);
        result.putAll(responseAttributes(throwable));
        return result;
    }
}
