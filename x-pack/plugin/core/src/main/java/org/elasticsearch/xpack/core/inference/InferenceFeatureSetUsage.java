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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class InferenceFeatureSetUsage extends XPackFeatureUsage {

    public static class ModelStats implements ToXContentObject, Writeable {

        private final String service;
        private final TaskType taskType;
        private long count;

        public ModelStats(String service, TaskType taskType) {
            this(service, taskType, 0L);
        }

        public ModelStats(String service, TaskType taskType, long count) {
            this.service = service;
            this.taskType = taskType;
            this.count = count;
        }

        public ModelStats(ModelStats stats) {
            this(stats.service, stats.taskType, stats.count);
        }

        public ModelStats(StreamInput in) throws IOException {
            this.service = in.readString();
            this.taskType = in.readEnum(TaskType.class);
            this.count = in.readLong();
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
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(service);
            out.writeEnum(taskType);
            out.writeLong(count);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ModelStats that = (ModelStats) o;
            return count == that.count && Objects.equals(service, that.service) && taskType == that.taskType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(service, taskType, count);
        }
    }

    public static class SemanticTextStats implements ToXContentObject, Writeable {
        private final Long totalFieldCount;
        private final Long indexCount;
        private final Long denseFieldCount;
        private final Long sparseFieldCount;
        private final Long denseInferenceIdCount;
        private final Long sparseInferenceIdCount;

        public SemanticTextStats(
            Long totalFieldCount,
            Long indexCount,
            Long sparseFieldCount,
            Long denseFieldCount,
            Long denseInferenceIdCount,
            Long sparseInferenceIdCount
        ) {
            this.totalFieldCount = totalFieldCount;
            this.indexCount = indexCount;
            this.sparseFieldCount = sparseFieldCount;
            this.denseFieldCount = denseFieldCount;
            this.denseInferenceIdCount = denseInferenceIdCount;
            this.sparseInferenceIdCount = sparseInferenceIdCount;
        }

        public SemanticTextStats(StreamInput in) throws IOException {
            this.totalFieldCount = in.readLong();
            this.indexCount = in.readLong();
            this.denseFieldCount = in.readLong();
            this.denseInferenceIdCount = in.readLong();
            this.sparseInferenceIdCount = in.readLong();
            this.sparseFieldCount = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(totalFieldCount);
            out.writeLong(indexCount);
            out.writeLong(denseFieldCount);
            out.writeLong(denseInferenceIdCount);
            out.writeLong(sparseInferenceIdCount);
            out.writeLong(sparseFieldCount);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("total_fields", totalFieldCount);
            builder.field("indices", indexCount);
            builder.field("dense_fields", denseFieldCount);
            builder.field("dense_inference_ids", denseInferenceIdCount);
            builder.field("sparse_fields", sparseFieldCount);
            builder.field("sparse_inference_ids", sparseInferenceIdCount);
            builder.endObject();
            return builder;
        }
    }

    public static final InferenceFeatureSetUsage EMPTY = new InferenceFeatureSetUsage(List.of(), null);

    private final Collection<ModelStats> modelStats;
    private final SemanticTextStats semanticTextStats;

    public InferenceFeatureSetUsage(Collection<ModelStats> modelStats, SemanticTextStats semanticTextStats) {
        super(XPackField.INFERENCE, true, true);
        this.modelStats = modelStats;
        this.semanticTextStats = semanticTextStats;
    }

    public InferenceFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.modelStats = in.readCollectionAsList(ModelStats::new);
        this.semanticTextStats = new SemanticTextStats(in);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.xContentList("models", modelStats);
        builder.field("semantic_text", semanticTextStats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(modelStats);
        semanticTextStats.writeTo(out);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        InferenceFeatureSetUsage that = (InferenceFeatureSetUsage) o;
        return Objects.equals(modelStats, that.modelStats) && Objects.equals(semanticTextStats, that.semanticTextStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelStats, semanticTextStats);
    }
}
