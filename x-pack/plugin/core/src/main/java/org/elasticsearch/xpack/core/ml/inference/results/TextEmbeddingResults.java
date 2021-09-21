/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class TextEmbeddingResults implements InferenceResults {

    public static final String NAME = "text_embedding_result";
    static final String DEFAULT_RESULTS_FIELD = "results";

    private static final ParseField INFERENCE = new ParseField("inference");

    private final double[] inference;

    public TextEmbeddingResults(double[] inference) {
        this.inference = inference;
    }

    public TextEmbeddingResults(StreamInput in) throws IOException {
        inference = in.readDoubleArray();
    }

    public double[] getInference() {
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
        out.writeDoubleArray(inference);
    }

    @Override
    public Map<String, Object> asMap() {
        return Collections.singletonMap(DEFAULT_RESULTS_FIELD, inference);
    }

    @Override
    public Object predictedValue() {
        throw new UnsupportedOperationException("[" + NAME + "] does not support a single predicted value");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextEmbeddingResults that = (TextEmbeddingResults) o;
        return Arrays.equals(inference, that.inference);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(inference);
    }
}
