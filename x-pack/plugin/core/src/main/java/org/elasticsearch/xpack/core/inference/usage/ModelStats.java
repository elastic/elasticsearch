/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.usage;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ModelStats implements ToXContentObject, Writeable {

    public static final NodeFeature SEMANTIC_TEXT_USAGE = new NodeFeature("inference.semantic_text_usage");

    static final TransportVersion INFERENCE_TELEMETRY_ADDED_SEMANTIC_TEXT_STATS = TransportVersion.fromName(
        "inference_telemetry_added_semantic_text_stats"
    );

    private final String service;
    private final TaskType taskType;
    private long count;
    @Nullable
    private final SemanticTextStats semanticTextStats;

    public ModelStats(String service, TaskType taskType, long count, @Nullable SemanticTextStats semanticTextStats) {
        this.service = service;
        this.taskType = taskType;
        this.count = count;
        this.semanticTextStats = semanticTextStats;
    }

    public ModelStats(StreamInput in) throws IOException {
        this.service = in.readString();
        this.taskType = in.readEnum(TaskType.class);
        this.count = in.readLong();
        if (in.getTransportVersion().supports(INFERENCE_TELEMETRY_ADDED_SEMANTIC_TEXT_STATS)) {
            this.semanticTextStats = in.readOptional(SemanticTextStats::new);
        } else {
            this.semanticTextStats = null;
        }
    }

    public void add() {
        count++;
    }

    public String service() {
        return service;
    }

    public TaskType taskType() {
        return taskType;
    }

    public long count() {
        return count;
    }

    @Nullable
    public SemanticTextStats semanticTextStats() {
        return semanticTextStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        addXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    public void addXContentFragment(XContentBuilder builder, Params params) throws IOException {
        builder.field("service", service);
        builder.field("task_type", taskType.name());
        builder.field("count", count);
        if (semanticTextStats != null) {
            builder.field("semantic_text", semanticTextStats);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(service);
        out.writeEnum(taskType);
        out.writeLong(count);
        if (out.getTransportVersion().supports(INFERENCE_TELEMETRY_ADDED_SEMANTIC_TEXT_STATS)) {
            out.writeOptionalWriteable(semanticTextStats);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModelStats that = (ModelStats) o;
        return count == that.count
            && Objects.equals(service, that.service)
            && taskType == that.taskType
            && Objects.equals(semanticTextStats, that.semanticTextStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(service, taskType, count, semanticTextStats);
    }
}
