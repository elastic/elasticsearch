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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class NlpClassificationInferenceResults extends NlpInferenceResults {

    public static final String NAME = "nlp_classification";

    // Accessed in sub-classes
    protected final String resultsField;
    private final String classificationLabel;
    private final Double predictionProbability;
    private final List<TopClassEntry> topClasses;

    public NlpClassificationInferenceResults(
        String classificationLabel,
        List<TopClassEntry> topClasses,
        String resultsField,
        Double predictionProbability,
        boolean isTruncated
    ) {
        super(isTruncated);
        this.classificationLabel = Objects.requireNonNull(classificationLabel);
        this.topClasses = topClasses == null ? Collections.emptyList() : Collections.unmodifiableList(topClasses);
        this.resultsField = resultsField;
        this.predictionProbability = predictionProbability;
    }

    public NlpClassificationInferenceResults(StreamInput in) throws IOException {
        super(in);
        this.classificationLabel = in.readString();
        this.topClasses = in.readImmutableList(TopClassEntry::new);
        this.resultsField = in.readString();
        this.predictionProbability = in.readOptionalDouble();
    }

    public String getClassificationLabel() {
        return classificationLabel;
    }

    public List<TopClassEntry> getTopClasses() {
        return topClasses;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(classificationLabel);
        out.writeCollection(topClasses);
        out.writeString(resultsField);
        out.writeOptionalDouble(predictionProbability);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        NlpClassificationInferenceResults that = (NlpClassificationInferenceResults) o;
        return Objects.equals(resultsField, that.resultsField)
            && Objects.equals(classificationLabel, that.classificationLabel)
            && Objects.equals(predictionProbability, that.predictionProbability)
            && Objects.equals(topClasses, that.topClasses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultsField, classificationLabel, predictionProbability, topClasses);
    }

    public Double getPredictionProbability() {
        return predictionProbability;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public Object predictedValue() {
        return classificationLabel;
    }

    @Override
    void addMapFields(Map<String, Object> map) {
        map.put(resultsField, classificationLabel);
        if (topClasses.isEmpty() == false) {
            map.put(
                NlpConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD,
                topClasses.stream().map(TopClassEntry::asValueMap).collect(Collectors.toList())
            );
        }
        if (predictionProbability != null) {
            map.put(PREDICTION_PROBABILITY, predictionProbability);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(resultsField, classificationLabel);
        if (topClasses.size() > 0) {
            builder.field(NlpConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD, topClasses);
        }
        if (predictionProbability != null) {
            builder.field(PREDICTION_PROBABILITY, predictionProbability);
        }
    }
}
