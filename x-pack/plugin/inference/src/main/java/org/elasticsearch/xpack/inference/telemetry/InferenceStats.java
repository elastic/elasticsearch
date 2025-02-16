/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;

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

    private static Map<String, Object> toMap(Stream<Map.Entry<String, Object>> stream) {
        return stream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<String, Object> modelAttributes(Model model) {
        var stream = Stream.<Map.Entry<String, Object>>builder()
            .add(entry("service", model.getConfigurations().getService()))
            .add(entry("task_type", model.getTaskType().toString()));
        if (model.getServiceSettings().modelId() != null) {
            stream.add(entry("model_id", model.getServiceSettings().modelId()));
        }
        return toMap(stream.build());
    }

    public static Map<String, Object> routingAttributes(BaseInferenceActionRequest request, String nodeIdHandlingRequest) {
        return Map.of("rerouted", request.hasBeenRerouted(), "node_id", nodeIdHandlingRequest);
    }

    public static Map<String, Object> modelAttributes(UnparsedModel model) {
        var unknownModelAttributes = Stream.<Map.Entry<String, Object>>builder()
            .add(entry("service", model.service()))
            .add(entry("task_type", model.taskType().toString()))
            .build();

        return toMap(unknownModelAttributes);
    }

    public static Map<String, Object> responseAttributes(@Nullable Throwable t) {
        var stream = switch (t) {
            case null -> Stream.<Map.Entry<String, Object>>of(entry("status_code", 200));
            case ElasticsearchStatusException ese -> Stream.<Map.Entry<String, Object>>builder()
                .add(entry("status_code", ese.status().getStatus()))
                .add(entry("error.type", String.valueOf(ese.status().getStatus())))
                .build();
            default -> Stream.<Map.Entry<String, Object>>of(entry("error.type", t.getClass().getSimpleName()));
        };

        return toMap(stream);
    }
}
