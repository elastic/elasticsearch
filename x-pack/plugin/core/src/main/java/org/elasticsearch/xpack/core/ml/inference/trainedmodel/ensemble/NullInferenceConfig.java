/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;

import java.io.IOException;

/**
 * Used by ensemble to pass into sub-models.
 */
class NullInferenceConfig implements InferenceConfig {

    public static final NullInferenceConfig INSTANCE = new NullInferenceConfig();

    private NullInferenceConfig() { }

    @Override
    public boolean isTargetTypeSupported(TargetType targetType) {
        return true;
    }

    @Override
    public String getWriteableName() {
        return "null";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public String getName() {
        return "null";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }
}
