/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * TencentCloud embeddings do not support any task-level settings today. This class is kept as an empty implementation to
 * remain compatible with the {@link org.elasticsearch.inference.ModelConfigurations} contract.
 */
public class TencentCloudEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "tencentcloud_embeddings_task_settings";
    public static final TencentCloudEmbeddingsTaskSettings EMPTY_SETTINGS = new TencentCloudEmbeddingsTaskSettings();

    public static TencentCloudEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        return EMPTY_SETTINGS;
    }

    public TencentCloudEmbeddingsTaskSettings() {}

    public TencentCloudEmbeddingsTaskSettings(StreamInput in) throws IOException {
        // no fields to read
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // no fields to write
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o != null && getClass() == o.getClass());
    }

    @Override
    public int hashCode() {
        return NAME.hashCode();
    }
}
