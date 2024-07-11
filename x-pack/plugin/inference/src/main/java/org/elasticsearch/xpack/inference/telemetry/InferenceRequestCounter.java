/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.InferenceRequestStats;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public class InferenceRequestCounter implements Counter, Serializable<InferenceRequestStats> {
    private final String service;
    private final TaskType taskType;
    private final LongAdder counter = new LongAdder();
    private final String modelId;

    public static String key(Model model) {
        StringBuilder builder = new StringBuilder();
        builder.append(model.getConfigurations().getService());
        builder.append(":");
        builder.append(model.getTaskType());

        if (model.getServiceSettings().modelId() != null) {
            builder.append(":");
            builder.append(model.getServiceSettings().modelId());
        }

        return builder.toString();
    }

    public InferenceRequestCounter(Model model) {
        Objects.requireNonNull(model);

        service = model.getConfigurations().getService();
        taskType = model.getTaskType();
        modelId = model.getServiceSettings().modelId();
    }

    public InferenceRequestCounter(StreamInput in) throws IOException {
        this.service = in.readString();
        this.taskType = in.readEnum(TaskType.class);
        this.modelId = in.readOptionalString();
        var count = in.readVLong();
        this.counter.add(count);
    }

    @Override
    public void increment() {
        counter.increment();
    }

    @Override
    public long getCount() {
        return counter.sum();
    }

    @Override
    public InferenceRequestStats convert() {
        return new InferenceRequestStats(service, taskType, modelId, counter.sum());
    }
}
