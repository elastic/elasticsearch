/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public class InferenceRequestStats
    implements
        Stats,
        Transformable<org.elasticsearch.xpack.core.inference.InferenceRequestStats>,
        Closeable {
    protected final String service;
    protected final TaskType taskType;
    protected final String modelId;
    private final LongAdder counter = new LongAdder();

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

    public InferenceRequestStats(Model model) {
        Objects.requireNonNull(model);

        service = model.getConfigurations().getService();
        taskType = model.getTaskType();
        modelId = model.getServiceSettings().modelId();
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
    public org.elasticsearch.xpack.core.inference.InferenceRequestStats transform() {
        return new org.elasticsearch.xpack.core.inference.InferenceRequestStats(service, taskType, modelId, getCount());
    }

    @Override
    public void close() throws IOException {}
}
