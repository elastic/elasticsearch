/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.inference.Model;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
import java.util.Objects;

public class ApmInferenceStats implements InferenceStats {
    private final LongCounter inferenceAPMRequestCounter;

    public ApmInferenceStats(LongCounter inferenceAPMRequestCounter) {
        this.inferenceAPMRequestCounter = Objects.requireNonNull(inferenceAPMRequestCounter);
    }

    @Override
    public void incrementRequestCount(Model model) {
        var service = model.getConfigurations().getService();
        var taskType = model.getTaskType();
        var modelId = model.getServiceSettings().modelId();

        var attributes = new HashMap<String, Object>(5);
        attributes.put("service", service);
        attributes.put("task_type", taskType.toString());
        if (modelId != null) {
            attributes.put("model_id", modelId);
        }

        inferenceAPMRequestCounter.incrementBy(1, attributes);
    }

    public static ApmInferenceStats create(MeterRegistry meterRegistry) {
        return new ApmInferenceStats(
            meterRegistry.registerLongCounter(
                "es.inference.requests.count.total",
                "Inference API request counts for a particular service, task type, model ID",
                "operations"
            )
        );
    }
}
