/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class InferenceRequestStats implements ToXContentObject, Writeable {
    private final InferenceFeatureSetUsage.ModelStats modelStats;
    private final String modelId;

    public InferenceRequestStats(String service, TaskType taskType, String modelId) {
        this(new InferenceFeatureSetUsage.ModelStats(service, taskType, 0L), modelId);
    }

    InferenceRequestStats(InferenceFeatureSetUsage.ModelStats modelStats, String modelId) {
        this.modelStats = new InferenceFeatureSetUsage.ModelStats(modelStats);
        this.modelId = modelId;
    }

    public InferenceRequestStats(InferenceRequestStats stats) {
        this(stats.modelStats, stats.modelId);
    }

    public InferenceRequestStats(StreamInput in) throws IOException {
        this.modelStats = new InferenceFeatureSetUsage.ModelStats(in);
        this.modelId = in.readString();
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        modelStats.addXContentFragment(builder, params);
        builder.field("model_id", modelId);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        modelStats.writeTo(out);
        out.writeString(modelId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceRequestStats that = (InferenceRequestStats) o;
        return Objects.equals(modelStats, that.modelStats) && Objects.equals(modelId, that.modelId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelStats, modelId);
    }
}
