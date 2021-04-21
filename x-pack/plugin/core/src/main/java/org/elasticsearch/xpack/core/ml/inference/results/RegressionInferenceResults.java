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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class RegressionInferenceResults extends SingleValueInferenceResults {

    public static final String NAME = "regression";

    private final String resultsField;
    private final List<RegressionFeatureImportance> featureImportance;

    public RegressionInferenceResults(double value, InferenceConfig config) {
        this(value, config, Collections.emptyList());
    }

    public RegressionInferenceResults(double value, InferenceConfig config, List<RegressionFeatureImportance> featureImportance) {
        this(
            value,
            ((RegressionConfig)config).getResultsField(),
            ((RegressionConfig)config).getNumTopFeatureImportanceValues(),
            featureImportance
        );
    }

    public RegressionInferenceResults(double value, String resultsField) {
        this(value, resultsField, 0, Collections.emptyList());
    }

    public RegressionInferenceResults(double value, String resultsField,
                                      List<RegressionFeatureImportance> featureImportance) {
        this(value, resultsField, featureImportance.size(), featureImportance);
    }

    public RegressionInferenceResults(double value, String resultsField, int topNFeatures,
                                       List<RegressionFeatureImportance> featureImportance) {
        super(value);
        this.resultsField = resultsField;
        this.featureImportance = takeTopFeatureImportances(featureImportance, topNFeatures);
    }

    static List<RegressionFeatureImportance> takeTopFeatureImportances(List<RegressionFeatureImportance> featureImportances,
                                                                       int numTopFeatures) {
        if (featureImportances == null || featureImportances.isEmpty()) {
            return Collections.emptyList();
        }
        return featureImportances.stream()
            .sorted((l, r)-> Double.compare(Math.abs(r.getImportance()), Math.abs(l.getImportance())))
            .limit(numTopFeatures)
            .collect(Collectors.toUnmodifiableList());
    }

    public RegressionInferenceResults(StreamInput in) throws IOException {
        super(in);
        this.featureImportance = in.readList(RegressionFeatureImportance::new);
        this.resultsField = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(featureImportance);
        out.writeString(resultsField);
    }

    public List<RegressionFeatureImportance> getFeatureImportance() {
        return featureImportance;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        RegressionInferenceResults that = (RegressionInferenceResults) object;
        return Objects.equals(value(), that.value())
            && Objects.equals(this.resultsField, that.resultsField)
            && Objects.equals(this.featureImportance, that.featureImportance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value(), resultsField, featureImportance);
    }

    @Override
    public Object predictedValue() {
        return super.value();
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(resultsField, value());
        if (featureImportance.isEmpty() == false) {
            map.put(FEATURE_IMPORTANCE, featureImportance.stream().map(RegressionFeatureImportance::toMap).collect(Collectors.toList()));
        }
        return map;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(resultsField, value());
        if (featureImportance.isEmpty() == false) {
            builder.field(FEATURE_IMPORTANCE, featureImportance);
        }
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
