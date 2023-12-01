/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class InferenceFeatureSetUsage extends XPackFeatureSet.Usage {

    public static class ModelStats {

        private final String service;
        private final TaskType taskType;
        private long count;

        public ModelStats(String service, TaskType taskType) {
            this.service = service;
            this.taskType = taskType;
            this.count = 0;
        }

        public void add() {
            count++;
        }

        Map<String, Object> asMap() {
            return Map.of(
                "service", service,
                "task_type", taskType.name(),
                "count", count
            );
        }
    }

    private final Map<String, Object> modelStats;

    public InferenceFeatureSetUsage(boolean available, boolean enabled, Collection<ModelStats> modelStats) {
        super(XPackField.INFERENCE, available, enabled);
        this.modelStats = Map.of("models", modelStats.stream().map(ModelStats::asMap).collect(Collectors.toList()));
    }

    public InferenceFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.modelStats = input.readMap();
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (enabled) {
            builder.mapContents(modelStats);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(modelStats);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_8_1;
    }
}
