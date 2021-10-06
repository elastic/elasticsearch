/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PredictionFieldType;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FillMaskResults extends ClassificationInferenceResults {

    public static final String NAME = "fill_mask_result";

    private final String predictedSequence;

    public FillMaskResults(
        double value,
        String classificationLabel,
        String predictedSequence,
        List<TopClassEntry> topClasses,
        String topNumClassesField,
        String resultsField,
        Double predictionProbability
    ) {
        super(
            value,
            classificationLabel,
            topClasses,
            List.of(),
            topNumClassesField,
            resultsField,
            PredictionFieldType.STRING,
            0,
            predictionProbability,
            null
        );
        this.predictedSequence = predictedSequence;
    }

    public FillMaskResults(StreamInput in) throws IOException {
        super(in);
        this.predictedSequence = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(predictedSequence);
    }

    public String getPredictedSequence() {
        return predictedSequence;
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(resultsField + "_sequence", predictedSequence);
        map.putAll(super.asMap());
        return map;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return super.toXContent(builder, params).field(resultsField + "_sequence", predictedSequence);
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
