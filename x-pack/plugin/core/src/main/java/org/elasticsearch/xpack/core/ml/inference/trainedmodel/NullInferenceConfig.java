/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Used by ensemble to pass into sub-models.
 */
public class NullInferenceConfig implements InferenceConfig {

    private final boolean requestingFeatureImportance;

    public NullInferenceConfig(boolean requestingFeatureImportance) {
        this.requestingFeatureImportance = requestingFeatureImportance;
    }

    @Override
    public boolean isTargetTypeSupported(TargetType targetType) {
        return true;
    }

    @Override
    public Version getMinimalSupportedNodeVersion() {
        return Version.CURRENT;
    }

    @Override
    public TransportVersion getMinimalSupportedTransportVersion() {
        return TransportVersion.current();
    }

    @Override
    public String getWriteableName() {
        return "null";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Unable to serialize NullInferenceConfig objects");
    }

    @Override
    public String getName() {
        return "null";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("Unable to write xcontent from NullInferenceConfig objects");
    }

    @Override
    public boolean requestingImportance() {
        return requestingFeatureImportance;
    }

    @Override
    public boolean isAllocateOnly() {
        return false;
    }

    @Override
    public String getResultsField() {
        return null;
    }
}
