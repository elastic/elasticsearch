/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public record InferenceStats(LongCounter requestCount, LongHistogram inferenceDuration) {

    public InferenceStats {
        Objects.requireNonNull(requestCount);
        Objects.requireNonNull(inferenceDuration);
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
            )
        );
    }

    public static Map<String, Object> modelAttributes(Model model) {
        var modelAttributesMap = new HashMap<String, Object>();
        modelAttributesMap.put("service", model.getConfigurations().getService());
        modelAttributesMap.put("task_type", model.getTaskType().toString());

        if (Objects.nonNull(model.getServiceSettings().modelId())) {
            modelAttributesMap.put("model_id", model.getServiceSettings().modelId());
        }

        return modelAttributesMap;
    }

    public static Map<String, Object> routingAttributes(BaseInferenceActionRequest request, String nodeIdHandlingRequest) {
        return Map.of("rerouted", request.hasBeenRerouted(), "node_id", nodeIdHandlingRequest);
    }

    public static Map<String, Object> modelAttributes(UnparsedModel model) {
        return Map.of("service", model.service(), "task_type", model.taskType().toString());
    }

    public static Map<String, Object> responseAttributes(@Nullable Throwable throwable) {
        if (Objects.isNull(throwable)) {
            return Map.of("status_code", 200);
        }

        if (throwable instanceof ElasticsearchStatusException ese) {
            return Map.of("status_code", ese.status().getStatus(), "error.type", String.valueOf(ese.status().getStatus()));
        }

        return Map.of("error.type", throwable.getClass().getSimpleName());
    }
}
