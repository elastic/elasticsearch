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

    public static final InferenceFeatureSetUsage EMPTY = new InferenceFeatureSetUsage(List.of());

    private final Collection<ModelStats> modelStats;

    public InferenceFeatureSetUsage(Collection<ModelStats> modelStats) {
        super(XPackField.INFERENCE, true, true);
        this.modelStats = modelStats;
    }

    public InferenceFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.modelStats = in.readCollectionAsList(ModelStats::new);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.xContentList("models", modelStats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(modelStats);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        InferenceFeatureSetUsage that = (InferenceFeatureSetUsage) o;
        return Objects.equals(modelStats, that.modelStats);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(modelStats);
    }
}
