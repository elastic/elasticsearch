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
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Collection;

public class InferenceFeatureSetUsage extends XPackFeatureSet.Usage {

    public static class ModelStats implements ToXContentObject, Writeable {

        private final String service;
        private final TaskType taskType;
        private long count;

        public ModelStats(String service, TaskType taskType) {
            this.service = service;
            this.taskType = taskType;
            this.count = 0;
        }

        public ModelStats(StreamInput in) throws IOException {
            this.service = in.readString();
            this.taskType = in.readEnum(TaskType.class);
            this.count = in.readLong();
        }

        public void add() {
            count++;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("service", service);
            builder.field("task_type", taskType.name());
            builder.field("count", count);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(service);
            out.writeEnum(taskType);
            out.writeLong(count);
        }
    }

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
        return TransportVersions.INFERENCE_USAGE_ADDED;
    }
}
