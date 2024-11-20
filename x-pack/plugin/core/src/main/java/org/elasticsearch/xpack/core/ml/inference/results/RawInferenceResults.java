/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class RawInferenceResults implements InferenceResults {

    public static final String NAME = "raw";

    private final double[] value;
    private final double[][] featureImportance;

    public RawInferenceResults(double[] value, double[][] featureImportance) {
        this.value = value;
        this.featureImportance = featureImportance;
    }

    public double[] getValue() {
        return value;
    }

    public double[][] getFeatureImportance() {
        return featureImportance;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("[raw] does not support wire serialization");
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        RawInferenceResults that = (RawInferenceResults) object;
        return Arrays.equals(value, that.value) && Arrays.deepEquals(featureImportance, that.featureImportance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(value), Arrays.deepHashCode(featureImportance));
    }

    @Override
    public String getResultsField() {
        return DEFAULT_RESULTS_FIELD;
    }

    @Override
    public Map<String, Object> asMap() {
        throw new UnsupportedOperationException("[raw] does not support map conversion");
    }

    @Override
    public Map<String, Object> asMap(String outputField) {
        throw new UnsupportedOperationException("[raw] does not support map conversion");
    }

    @Override
    public Object predictedValue() {
        return null;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("[raw] does not support toXContent");
    }
}
