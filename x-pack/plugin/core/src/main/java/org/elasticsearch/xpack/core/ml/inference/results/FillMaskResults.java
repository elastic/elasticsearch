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
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FillMaskResults extends NlpClassificationInferenceResults {

    public static final String NAME = "fill_mask_result";

    private final String predictedSequence;

    public FillMaskResults(
        String classificationLabel,
        String predictedSequence,
        List<TopClassEntry> topClasses,
        String resultsField,
        Double predictionProbability,
        boolean isTruncated
    ) {
        super(classificationLabel, topClasses, resultsField, predictionProbability, isTruncated);
        this.predictedSequence = predictedSequence;
    }

    public FillMaskResults(StreamInput in) throws IOException {
        super(in);
        this.predictedSequence = in.readString();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeString(predictedSequence);
    }

    public String getPredictedSequence() {
        return predictedSequence;
    }

    @Override
    void addMapFields(Map<String, Object> map) {
        super.addMapFields(map);
        map.put(resultsField + "_sequence", predictedSequence);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        super.doXContentBody(builder, params);
        builder.field(resultsField + "_sequence", predictedSequence);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        FillMaskResults that = (FillMaskResults) o;
        return Objects.equals(predictedSequence, that.predictedSequence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), predictedSequence);
    }
}
