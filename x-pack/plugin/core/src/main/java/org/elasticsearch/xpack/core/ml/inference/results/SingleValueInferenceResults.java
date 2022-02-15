/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class SingleValueInferenceResults implements InferenceResults {

    public static final String FEATURE_IMPORTANCE = "feature_importance";

    private final double value;

    SingleValueInferenceResults(StreamInput in) throws IOException {
        value = in.readDouble();
    }

    SingleValueInferenceResults(double value) {
        this.value = value;
    }

    public Double value() {
        return value;
    }

    public String valueAsString() {
        return String.valueOf(value);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(value);
    }

}
