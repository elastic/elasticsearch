/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ClassificationConfig implements InferenceConfig {

    public static final ParseField NAME = new ParseField("classification");

    public static final String DEFAULT_TOP_CLASSES_RESULTS_FIELD = "top_classes";
    private static final String DEFAULT_RESULTS_FIELD = "predicted_value";

    public static final ParseField RESULTS_FIELD = new ParseField("results_field");
    public static final ParseField NUM_TOP_CLASSES = new ParseField("num_top_classes");
    public static final ParseField TOP_CLASSES_RESULTS_FIELD = new ParseField("top_classes_results_field");
    public static final ParseField NUM_TOP_FEATURE_IMPORTANCE_VALUES = new ParseField("num_top_feature_importance_values");
    private static final Version MIN_SUPPORTED_VERSION = Version.V_7_6_0;

    public static ClassificationConfig EMPTY_PARAMS =
        new ClassificationConfig(0, DEFAULT_RESULTS_FIELD, DEFAULT_TOP_CLASSES_RESULTS_FIELD, null);

    private final int numTopClasses;
    private final String topClassesResultsField;
    private final String resultsField;
    private final int numTopFeatureImportanceValues;

    public static ClassificationConfig fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        Integer numTopClasses = (Integer)options.remove(NUM_TOP_CLASSES.getPreferredName());
        String topClassesResultsField = (String)options.remove(TOP_CLASSES_RESULTS_FIELD.getPreferredName());
        String resultsField = (String)options.remove(RESULTS_FIELD.getPreferredName());
        Integer featureImportance = (Integer)options.remove(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName());

        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", options.keySet());
        }
        return new ClassificationConfig(numTopClasses, resultsField, topClassesResultsField, featureImportance);
    }

    private static final ConstructingObjectParser<ClassificationConfig, Void> PARSER =
            new ConstructingObjectParser<>(NAME.getPreferredName(), args -> new ClassificationConfig(
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

    public ClassificationConfig(Integer numTopClasses) {
        this(numTopClasses, null, null, null);
    }

    public ClassificationConfig(Integer numTopClasses, String resultsField, String topClassesResultsField) {
        this(numTopClasses, resultsField, topClassesResultsField, 0);
    }

    public ClassificationConfig(Integer numTopClasses, String resultsField, String topClassesResultsField, Integer featureImportance) {
        this.numTopClasses = numTopClasses == null ? 0 : numTopClasses;
        this.topClassesResultsField = topClassesResultsField == null ? DEFAULT_TOP_CLASSES_RESULTS_FIELD : topClassesResultsField;
        this.resultsField = resultsField == null ? DEFAULT_RESULTS_FIELD : resultsField;
        if (featureImportance != null && featureImportance < 0) {
            throw new IllegalArgumentException("[" + NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName() +
                "] must be greater than or equal to 0");
        }
        this.numTopFeatureImportanceValues = featureImportance == null ? 0 : featureImportance;
    }

    public ClassificationConfig(StreamInput in) throws IOException {
        this.numTopClasses = in.readInt();
        this.topClassesResultsField = in.readString();
        this.resultsField = in.readString();
        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            this.numTopFeatureImportanceValues = in.readVInt();
        } else {
            this.numTopFeatureImportanceValues = 0;
        }
    }

    public int getNumTopClasses() {
        return numTopClasses;
    }

    public String getTopClassesResultsField() {
        return topClassesResultsField;
    }

    public String getResultsField() {
        return resultsField;
    }

    public int getNumTopFeatureImportanceValues() {
        return numTopFeatureImportanceValues;
    }

    @Override
    public boolean requestingImportance() {
        return numTopFeatureImportanceValues > 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(numTopClasses);
        out.writeString(topClassesResultsField);
        out.writeString(resultsField);
        if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
            out.writeVInt(numTopFeatureImportanceValues);
        }
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (numTopClasses != 0) {
            builder.field(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        }
        builder.field(TOP_CLASSES_RESULTS_FIELD.getPreferredName(), topClassesResultsField);
        builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        if (numTopFeatureImportanceValues > 0) {
            builder.field(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), numTopFeatureImportanceValues);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public boolean isTargetTypeSupported(TargetType targetType) {
        return TargetType.CLASSIFICATION.equals(targetType);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return numTopFeatureImportanceValues > 0 ? Version.V_7_7_0 : MIN_SUPPORTED_VERSION;
    }

}
