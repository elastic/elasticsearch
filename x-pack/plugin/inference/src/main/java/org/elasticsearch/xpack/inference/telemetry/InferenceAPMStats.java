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

import java.util.Objects;

public class InferenceAPMStats extends InferenceStats {

    private final LongCounter inferenceAPMRequestCounter;

    public InferenceAPMStats(Model model, MeterRegistry meterRegistry) {
        super(model);
        this.inferenceAPMRequestCounter = meterRegistry.registerLongCounter(
            counterName(),
            "Inference API request counts for a particular service, task type, model ID",
            "operations"
        );
    }

    String counterName() {
        StringBuilder builder = new StringBuilder();
        builder.append("es.inference.requests.");
        builder.append(service);
        builder.append(".");
        builder.append(taskType.toString());

        if (modelId != null) {
            builder.append(".");
            builder.append(modelId);
        }

        builder.append(".count");
        return builder.toString();
    }

    @Override
    public synchronized void increment() {
        super.increment();
        inferenceAPMRequestCounter.increment();
    }

    public static final class Factory {
        private final MeterRegistry meterRegistry;

        public Factory(MeterRegistry meterRegistry) {
            this.meterRegistry = Objects.requireNonNull(meterRegistry);
        }

        public InferenceAPMStats newInferenceRequestAPMCounter(Model model) {
            return new InferenceAPMStats(model, meterRegistry);
        }
    }
}
