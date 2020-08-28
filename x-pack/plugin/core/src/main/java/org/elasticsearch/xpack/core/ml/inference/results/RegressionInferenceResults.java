/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

    public RegressionInferenceResults(double value, InferenceConfig config) {
        this(value, config, Collections.emptyList());
    }

    public RegressionInferenceResults(double value, InferenceConfig config, List<FeatureImportance> featureImportance) {
        this(value, ((RegressionConfig)config).getResultsField(),
            ((RegressionConfig)config).getNumTopFeatureImportanceValues(), featureImportance);
    }

    public RegressionInferenceResults(double value, String resultsField) {
        this(value, resultsField, 0, Collections.emptyList());
    }

    public RegressionInferenceResults(double value, String resultsField,
                                      List<FeatureImportance> featureImportance) {
        this(value, resultsField, featureImportance.size(), featureImportance);
    }

    public RegressionInferenceResults(double value, String resultsField, int topNFeatures,
                                       List<FeatureImportance> featureImportance) {
        super(value,
            SingleValueInferenceResults.takeTopFeatureImportances(featureImportance, topNFeatures));
        this.resultsField = resultsField;
    }

    public RegressionInferenceResults(StreamInput in) throws IOException {
        super(in);
        this.resultsField = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(resultsField);
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        RegressionInferenceResults that = (RegressionInferenceResults) object;
        return Objects.equals(value(), that.value())
            && Objects.equals(this.resultsField, that.resultsField)
            && Objects.equals(this.getFeatureImportance(), that.getFeatureImportance());
    }

    @Override
    public int hashCode() {
        return Objects.hash(value(), resultsField, getFeatureImportance());
    }

    @Override
    public Object predictedValue() {
        return super.value();
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(resultsField, value());
        if (getFeatureImportance().isEmpty() == false) {
            map.put(FEATURE_IMPORTANCE, getFeatureImportance().stream().map(FeatureImportance::toMap).collect(Collectors.toList()));
        }
        return map;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(resultsField, value());
        if (getFeatureImportance().size() > 0) {
            builder.field(FEATURE_IMPORTANCE, getFeatureImportance());
        }
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
