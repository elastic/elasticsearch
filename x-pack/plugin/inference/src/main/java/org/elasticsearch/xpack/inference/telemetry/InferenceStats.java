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

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static java.util.stream.Stream.concat;

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
        return toMap(modelAttributeEntries(model));
    }

    private static Stream<Map.Entry<String, Object>> modelAttributeEntries(Model model) {
        var stream = Stream.<Map.Entry<String, Object>>builder()
            .add(entry("service", model.getConfigurations().getService()))
            .add(entry("task_type", model.getTaskType().toString()));
        if (model.getServiceSettings().modelId() != null) {
            stream.add(entry("model_id", model.getServiceSettings().modelId()));
        }
        return stream.build();
    }

    private static Map<String, Object> toMap(Stream<Map.Entry<String, Object>> stream) {
        return stream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<String, Object> responseAttributes(Model model, @Nullable Throwable t) {
        return toMap(concat(modelAttributeEntries(model), errorAttributes(t)));
    }

    public static Map<String, Object> responseAttributes(UnparsedModel model, @Nullable Throwable t) {
        var unknownModelAttributes = Stream.<Map.Entry<String, Object>>builder()
            .add(entry("service", model.service()))
            .add(entry("task_type", model.taskType().toString()))
            .build();

        return toMap(concat(unknownModelAttributes, errorAttributes(t)));
    }

    public static Map<String, Object> responseAttributes(@Nullable Throwable t) {
        return toMap(errorAttributes(t));
    }

    private static Stream<Map.Entry<String, Object>> errorAttributes(@Nullable Throwable t) {
        return switch (t) {
            case null -> Stream.of(entry("status_code", 200));
            case ElasticsearchStatusException ese -> Stream.<Map.Entry<String, Object>>builder()
                .add(entry("status_code", ese.status().getStatus()))
                .add(entry("error.type", String.valueOf(ese.status().getStatus())))
                .build();
            default -> Stream.of(entry("error.type", t.getClass().getSimpleName()));
        };
    }
}
