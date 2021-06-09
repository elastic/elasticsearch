/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class PyTorchPassThroughResults implements InferenceResults {

    public static final String NAME = "pass_through_result";
    static final String DEFAULT_RESULTS_FIELD = "results";

    private static final ParseField INFERENCE = new ParseField("inference");

    private final double[][] inference;

    public PyTorchPassThroughResults(double[][] inference) {
        this.inference = inference;
    }

    public PyTorchPassThroughResults(StreamInput in) throws IOException {
        inference = in.readArray(StreamInput::readDoubleArray, length -> new double[length][]);
    }

    public double[][] getInference() {
        return inference;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(INFERENCE.getPreferredName(), inference);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(StreamOutput::writeDoubleArray, inference);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(DEFAULT_RESULTS_FIELD, inference);
        return map;
    }

    @Override
    public Object predictedValue() {
        throw new UnsupportedOperationException("[" + NAME + "] does not support a single predicted value");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PyTorchPassThroughResults that = (PyTorchPassThroughResults) o;
        return Arrays.deepEquals(inference, that.inference);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(inference);
    }
}
