/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredTaskSchema;

import java.io.IOException;
import java.util.Map;

/**
 * SageMaker + Elastic task settings are different in that they will not be stored within the index because
 * they will not be verified. Instead, these task settings will only exist as additional input to SageMaker.
 */
record SageMakerElasticTaskSettings(@Nullable Map<String, Object> passthroughSettings) implements SageMakerStoredTaskSchema {
    static final String NAME = "sagemaker_elastic_task_settings";

    static SageMakerElasticTaskSettings empty() {
        return new SageMakerElasticTaskSettings(Map.of());
    }

    SageMakerElasticTaskSettings(StreamInput in) throws IOException {
        this(in.readGenericMap());
    }

    @Override
    public boolean isEmpty() {
        return passthroughSettings == null || passthroughSettings.isEmpty();
    }

    @Override
    public SageMakerStoredTaskSchema updatedTaskSettings(Map<String, Object> newSettings) {
        return new SageMakerElasticTaskSettings(newSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_SAGEMAKER_ELASTIC;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.onOrAfter(TransportVersions.ML_INFERENCE_SAGEMAKER_ELASTIC)
            || version.isPatchFrom(TransportVersions.ML_INFERENCE_SAGEMAKER_ELASTIC_8_19);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(passthroughSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return isEmpty() ? builder : builder.mapContents(passthroughSettings);
    }
}
