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

import java.util.Map;
import java.util.Objects;

public class InferenceAPMStats extends InferenceStats {

    private final LongCounter requestCounter;

    public InferenceAPMStats(Model model, LongCounter requestCounter) {
        super(model);
        this.requestCounter = Objects.requireNonNull(requestCounter);
    }

    @Override
    public void increment() {
        super.increment();
        requestCounter.incrementBy(1, Map.of("service", service, "task_type", taskType.toString(), "model_id", modelId));
    }

    public static final class Factory {
        private final LongCounter requestCounter;

        public Factory(MeterRegistry meterRegistry) {
            Objects.requireNonNull(meterRegistry);

            // A meter with a specific name can only be registered once
            this.requestCounter = meterRegistry.registerLongCounter(
                // We get an error if the name doesn't end with a specific value, total is a valid option
                "es.inference.requests.count.total",
                "Inference API request counts for a particular service, task type, model ID",
                "operations"
            );
        }

        public InferenceAPMStats newInferenceRequestAPMCounter(Model model) {
            return new InferenceAPMStats(model, requestCounter);
        }
    }
}
