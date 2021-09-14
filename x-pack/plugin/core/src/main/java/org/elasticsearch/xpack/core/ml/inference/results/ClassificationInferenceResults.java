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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PredictionFieldType;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ClassificationInferenceResults extends SingleValueInferenceResults {

    public static final String NAME = "classification";

    public static final String PREDICTION_PROBABILITY = "prediction_probability";
    public static final String PREDICTION_SCORE = "prediction_score";

    private final String topNumClassesField;
    private final String resultsField;
    private final String classificationLabel;
    private final Double predictionProbability;
    private final Double predictionScore;
    private final List<TopClassEntry> topClasses;
    private final List<ClassificationFeatureImportance> featureImportance;
    private final PredictionFieldType predictionFieldType;

    public ClassificationInferenceResults(double value,
                                          String classificationLabel,
                                          List<TopClassEntry> topClasses,
                                          List<ClassificationFeatureImportance> featureImportance,
                                          InferenceConfig config,
                                          Double predictionProbability,
                                          Double predictionScore) {
        this(value,
            classificationLabel,
            topClasses,
            featureImportance,
            (ClassificationConfig)config,
            predictionProbability,
            predictionScore);
    }

    private ClassificationInferenceResults(double value,
                                           String classificationLabel,
                                           List<TopClassEntry> topClasses,
                                           List<ClassificationFeatureImportance> featureImportance,
                                           ClassificationConfig classificationConfig,
                                           Double predictionProbability,
                                           Double predictionScore) {
        super(value);
        this.classificationLabel = classificationLabel;
        this.topClasses = topClasses == null ? Collections.emptyList() : Collections.unmodifiableList(topClasses);
        this.topNumClassesField = classificationConfig.getTopClassesResultsField();
        this.resultsField = classificationConfig.getResultsField();
        this.predictionFieldType = classificationConfig.getPredictionFieldType();
        this.predictionProbability = predictionProbability;
        this.predictionScore = predictionScore;
        this.featureImportance = takeTopFeatureImportances(featureImportance, classificationConfig.getNumTopFeatureImportanceValues());
    }

    static List<ClassificationFeatureImportance> takeTopFeatureImportances(List<ClassificationFeatureImportance> featureImportances,
                                                                           int numTopFeatures) {
        if (featureImportances == null || featureImportances.isEmpty()) {
            return Collections.emptyList();
        }
        return featureImportances.stream()
            .sorted((l, r)-> Double.compare(r.getTotalImportance(), l.getTotalImportance()))
            .limit(numTopFeatures)
            .collect(Collectors.toUnmodifiableList());
    }

    public ClassificationInferenceResults(StreamInput in) throws IOException {
        super(in);
        this.featureImportance = in.readList(ClassificationFeatureImportance::new);
        this.classificationLabel = in.readOptionalString();
        this.topClasses = Collections.unmodifiableList(in.readList(TopClassEntry::new));
        this.topNumClassesField = in.readString();
        this.resultsField = in.readString();
        this.predictionFieldType = in.readEnum(PredictionFieldType.class);
        this.predictionProbability = in.readOptionalDouble();
        this.predictionScore = in.readOptionalDouble();
    }

    public String getClassificationLabel() {
        return classificationLabel;
    }

    public List<TopClassEntry> getTopClasses() {
        return topClasses;
    }

    public PredictionFieldType getPredictionFieldType() {
        return predictionFieldType;
    }

    public List<ClassificationFeatureImportance> getFeatureImportance() {
        return featureImportance;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(featureImportance);
        out.writeOptionalString(classificationLabel);
        out.writeCollection(topClasses);
        out.writeString(topNumClassesField);
        out.writeString(resultsField);
        out.writeEnum(predictionFieldType);
        out.writeOptionalDouble(predictionProbability);
        out.writeOptionalDouble(predictionScore);
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        ClassificationInferenceResults that = (ClassificationInferenceResults) object;
        return Objects.equals(value(), that.value())
            && Objects.equals(classificationLabel, that.classificationLabel)
            && Objects.equals(resultsField, that.resultsField)
            && Objects.equals(topNumClassesField, that.topNumClassesField)
            && Objects.equals(topClasses, that.topClasses)
            && Objects.equals(predictionFieldType, that.predictionFieldType)
            && Objects.equals(predictionProbability, that.predictionProbability)
            && Objects.equals(predictionScore, that.predictionScore)
            && Objects.equals(featureImportance, that.featureImportance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value(),
            classificationLabel,
            topClasses,
            resultsField,
            topNumClassesField,
            predictionProbability,
            predictionScore,
            featureImportance,
            predictionFieldType);
    }

    @Override
    public String valueAsString() {
        return classificationLabel == null ? super.valueAsString() : classificationLabel;
    }

    @Override
    public Object predictedValue() {
        return predictionFieldType.transformPredictedValue(value(), valueAsString());
    }

    public Double getPredictionProbability() {
        return predictionProbability;
    }

    public Double getPredictionScore() {
        return predictionScore;
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(resultsField, predictionFieldType.transformPredictedValue(value(), valueAsString()));
        if (topClasses.isEmpty() == false) {
            map.put(topNumClassesField, topClasses.stream().map(TopClassEntry::asValueMap).collect(Collectors.toList()));
        }
        if (predictionProbability != null) {
            map.put(PREDICTION_PROBABILITY, predictionProbability);
        }
        if (predictionScore != null) {
            map.put(PREDICTION_SCORE, predictionScore);
        }
        if (featureImportance.isEmpty() == false) {
            map.put(FEATURE_IMPORTANCE, featureImportance.stream().map(ClassificationFeatureImportance::toMap)
                .collect(Collectors.toList()));
        }
        return map;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(resultsField, predictionFieldType.transformPredictedValue(value(), valueAsString()));
        if (topClasses.size() > 0) {
            builder.field(topNumClassesField, topClasses);
        }
        if (predictionProbability != null) {
            builder.field(PREDICTION_PROBABILITY, predictionProbability);
        }
        if (predictionScore != null) {
            builder.field(PREDICTION_SCORE, predictionScore);
        }
        if (featureImportance.isEmpty() == false) {
            builder.field(FEATURE_IMPORTANCE, featureImportance);
        }
        return builder;
    }
}
