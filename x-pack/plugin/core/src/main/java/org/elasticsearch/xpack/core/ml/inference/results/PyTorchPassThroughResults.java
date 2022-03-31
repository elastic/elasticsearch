/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class PyTorchPassThroughResults extends NlpInferenceResults {

    public static final String NAME = "pass_through_result";

    private final double[][] inference;
    private final String resultsField;

    public PyTorchPassThroughResults(String resultsField, double[][] inference, boolean isTruncated) {
        super(isTruncated);
        this.inference = inference;
        this.resultsField = resultsField;
    }

    public PyTorchPassThroughResults(StreamInput in) throws IOException {
        super(in);
        inference = in.readArray(StreamInput::readDoubleArray, double[][]::new);
        resultsField = in.readString();
    }

    public double[][] getInference() {
        return inference;
    }

    @Override
    void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(resultsField, inference);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeArray(StreamOutput::writeDoubleArray, inference);
        out.writeString(resultsField);
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    void addMapFields(Map<String, Object> map) {
        map.put(resultsField, inference);
    }

    @Override
    public Object predictedValue() {
        throw new UnsupportedOperationException("[" + NAME + "] does not support a single predicted value");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        PyTorchPassThroughResults that = (PyTorchPassThroughResults) o;
        return Arrays.deepEquals(inference, that.inference) && Objects.equals(resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultsField, Arrays.deepHashCode(inference));
    }
}
