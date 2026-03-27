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
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public record InferenceStats(LongCounter requestCount, LongHistogram inferenceDuration, LongHistogram deploymentDuration) {

    public InferenceStats {
        Objects.requireNonNull(requestCount);
        Objects.requireNonNull(inferenceDuration);
        Objects.requireNonNull(deploymentDuration);
    }

    public static InferenceStats create(MeterRegistry meterRegistry) {
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
            )
        );
    }

    public static Map<String, Object> serviceAttributes(Model model) {
        return Map.of("service", model.getConfigurations().getService(), "task_type", model.getTaskType().toString());
    }

    public static Map<String, Object> serviceAttributes(UnparsedModel model) {
        return Map.of("service", model.service(), "task_type", model.taskType().toString());
    }

    public static Map<String, Object> responseAttributes(@Nullable Throwable throwable) {
        if (Objects.isNull(throwable)) {
            return Map.of("status_code", 200);
        }

        if (throwable instanceof ElasticsearchStatusException ese) {
            return Map.of("status_code", ese.status().getStatus(), "error_type", String.valueOf(ese.status().getStatus()));
        }

        return Map.of("error_type", throwable.getClass().getSimpleName());
    }

    public static Map<String, Object> serviceAndResponseAttributes(Model model, @Nullable Throwable throwable) {
        var metricAttributes = new HashMap<String, Object>();
        metricAttributes.putAll(serviceAttributes(model));
        metricAttributes.putAll(responseAttributes(throwable));
        return metricAttributes;
    }
}
