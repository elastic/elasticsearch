/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.trainedmodel;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ClassificationConfig implements InferenceConfig {

    public static final ParseField NAME = new ParseField("classification");

    public static final ParseField RESULTS_FIELD = new ParseField("results_field");
    public static final ParseField NUM_TOP_CLASSES = new ParseField("num_top_classes");
    public static final ParseField TOP_CLASSES_RESULTS_FIELD = new ParseField("top_classes_results_field");
    public static final ParseField NUM_TOP_FEATURE_IMPORTANCE_VALUES = new ParseField("num_top_feature_importance_values");


    private final Integer numTopClasses;
    private final String topClassesResultsField;
    private final String resultsField;
    private final Integer numTopFeatureImportanceValues;

    private static final ConstructingObjectParser<ClassificationConfig, Void> PARSER =
            new ConstructingObjectParser<>(NAME.getPreferredName(), true, args -> new ClassificationConfig(
                    (Integer) args[0], (String) args[1], (String) args[2], (Integer) args[3]));

    static {
        PARSER.declareInt(optionalConstructorArg(), NUM_TOP_CLASSES);
        PARSER.declareString(optionalConstructorArg(), RESULTS_FIELD);
        PARSER.declareString(optionalConstructorArg(), TOP_CLASSES_RESULTS_FIELD);
        PARSER.declareInt(optionalConstructorArg(), NUM_TOP_FEATURE_IMPORTANCE_VALUES);
    }

    public static ClassificationConfig fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ClassificationConfig() {
        this(null, null, null, null);
    }

    public ClassificationConfig(Integer numTopClasses, String resultsField, String topClassesResultsField, Integer featureImportance) {
        this.numTopClasses = numTopClasses;
        this.topClassesResultsField = topClassesResultsField;
        this.resultsField = resultsField;
        this.numTopFeatureImportanceValues = featureImportance;
    }

    public Integer getNumTopClasses() {
        return numTopClasses;
    }

    public String getTopClassesResultsField() {
        return topClassesResultsField;
    }

    public String getResultsField() {
        return resultsField;
    }

    public Integer getNumTopFeatureImportanceValues() {
        return numTopFeatureImportanceValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClassificationConfig that = (ClassificationConfig) o;
        return Objects.equals(numTopClasses, that.numTopClasses)
            && Objects.equals(topClassesResultsField, that.topClassesResultsField)
            && Objects.equals(resultsField, that.resultsField)
            && Objects.equals(numTopFeatureImportanceValues, that.numTopFeatureImportanceValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numTopClasses, topClassesResultsField, resultsField, numTopFeatureImportanceValues);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (numTopClasses != null) {
            builder.field(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        }
        if (topClassesResultsField != null) {
            builder.field(TOP_CLASSES_RESULTS_FIELD.getPreferredName(), topClassesResultsField);
        }
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        if (numTopFeatureImportanceValues != null) {
            builder.field(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), numTopFeatureImportanceValues);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

}
