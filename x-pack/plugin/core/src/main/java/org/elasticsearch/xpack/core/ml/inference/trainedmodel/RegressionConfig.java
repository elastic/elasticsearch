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

public class RegressionConfig implements InferenceConfig {

    public static final ParseField NAME = new ParseField("regression");
    private static final Version MIN_SUPPORTED_VERSION = Version.V_7_6_0;
    public static final ParseField RESULTS_FIELD = new ParseField("results_field");
    public static final ParseField NUM_TOP_FEATURE_IMPORTANCE_VALUES = new ParseField("num_top_feature_importance_values");
    private static final String DEFAULT_RESULTS_FIELD = "predicted_value";

    public static RegressionConfig EMPTY_PARAMS = new RegressionConfig(DEFAULT_RESULTS_FIELD, null);

    public static RegressionConfig fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        String resultsField = (String)options.remove(RESULTS_FIELD.getPreferredName());
        Integer featureImportance = (Integer)options.remove(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName());
        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", map.keySet());
        }
        return new RegressionConfig(resultsField, featureImportance);
    }

    private static final ConstructingObjectParser<RegressionConfig, Void> PARSER =
            new ConstructingObjectParser<>(NAME.getPreferredName(), args -> new RegressionConfig((String) args[0], (Integer)args[1]));

    static {
        PARSER.declareString(optionalConstructorArg(), RESULTS_FIELD);
        PARSER.declareInt(optionalConstructorArg(), NUM_TOP_FEATURE_IMPORTANCE_VALUES);
    }

    public static RegressionConfig fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String resultsField;
    private final int numTopFeatureImportanceValues;

    public RegressionConfig(String resultsField) {
        this(resultsField, 0);
    }

    public RegressionConfig(String resultsField, Integer numTopFeatureImportanceValues) {
        this.resultsField = resultsField == null ? DEFAULT_RESULTS_FIELD : resultsField;
        if (numTopFeatureImportanceValues != null && numTopFeatureImportanceValues < 0) {
            throw new IllegalArgumentException("[" + NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName() +
                "] must be greater than or equal to 0");
        }
        this.numTopFeatureImportanceValues = numTopFeatureImportanceValues == null ? 0 : numTopFeatureImportanceValues;
    }

    public RegressionConfig(StreamInput in) throws IOException {
        this.resultsField = in.readString();
        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            this.numTopFeatureImportanceValues = in.readVInt();
        } else {
            this.numTopFeatureImportanceValues = 0;
        }
    }

    public int getNumTopFeatureImportanceValues() {
        return numTopFeatureImportanceValues;
    }

    public String getResultsField() {
        return resultsField;
    }

    @Override
    public boolean requestingImportance() {
        return numTopFeatureImportanceValues > 0;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(resultsField);
        if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
            out.writeVInt(numTopFeatureImportanceValues);
        }
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        if (numTopFeatureImportanceValues > 0) {
            builder.field(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), numTopFeatureImportanceValues);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegressionConfig that = (RegressionConfig)o;
        return Objects.equals(this.resultsField, that.resultsField)
            && Objects.equals(this.numTopFeatureImportanceValues, that.numTopFeatureImportanceValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultsField, numTopFeatureImportanceValues);
    }

    @Override
    public boolean isTargetTypeSupported(TargetType targetType) {
        return TargetType.REGRESSION.equals(targetType);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return numTopFeatureImportanceValues > 0 ? Version.V_7_7_0 : MIN_SUPPORTED_VERSION;
    }

}
