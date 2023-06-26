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
import java.util.Map;
import java.util.Objects;

public class TextSimilarityInferenceResults extends NlpInferenceResults {

    public static final String NAME = "text_similarity";

    private final String resultsField;
    private final double score;

    public TextSimilarityInferenceResults(String resultsField, double score, boolean isTruncated) {
        super(isTruncated);
        this.resultsField = resultsField;
        this.score = score;
    }

    public TextSimilarityInferenceResults(StreamInput in) throws IOException {
        super(in);
        this.resultsField = in.readString();
        this.score = in.readDouble();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(resultsField);
        out.writeDouble(score);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        TextSimilarityInferenceResults that = (TextSimilarityInferenceResults) o;
        return Objects.equals(resultsField, that.resultsField) && Objects.equals(score, that.score);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultsField, score);
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public Double predictedValue() {
        return score;
    }

    @Override
    void addMapFields(Map<String, Object> map) {
        map.put(resultsField, score);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(resultsField, score);
    }

}
