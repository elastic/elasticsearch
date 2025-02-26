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

public class MlTextEmbeddingResults extends NlpInferenceResults {

    public static final String NAME = "text_embedding_result";

    private final String resultsField;
    private final double[] inference;

    public MlTextEmbeddingResults(String resultsField, double[] inference, boolean isTruncated) {
        super(isTruncated);
        this.inference = inference;
        this.resultsField = resultsField;
    }

    public MlTextEmbeddingResults(StreamInput in) throws IOException {
        super(in);
        inference = in.readDoubleArray();
        resultsField = in.readString();
    }

    public String getResultsField() {
        return resultsField;
    }

    public double[] getInference() {
        return inference;
    }

    public float[] getInferenceAsFloat() {
        float[] floatArray = new float[inference.length];
        for (int i = 0; i < inference.length; i++) {
            floatArray[i] = (float) inference[i];
        }
        return floatArray;
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
    void doWriteTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(inference);
        out.writeString(resultsField);
    }

    @Override
    void addMapFields(Map<String, Object> map) {
        map.put(resultsField, inference);
    }

    @Override
    public Map<String, Object> asMap(String outputField) {
        var map = super.asMap(outputField);
        map.put(outputField, inference);
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
        if (super.equals(o) == false) return false;
        MlTextEmbeddingResults that = (MlTextEmbeddingResults) o;
        return Objects.equals(resultsField, that.resultsField) && Arrays.equals(inference, that.inference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultsField, Arrays.hashCode(inference));
    }
}
