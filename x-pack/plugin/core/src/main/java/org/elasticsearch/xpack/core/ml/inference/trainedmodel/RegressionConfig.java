/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;

import java.io.IOException;
import java.util.Objects;

public class RegressionConfig implements LenientlyParsedInferenceConfig, StrictlyParsedInferenceConfig {

    public static final ParseField NAME = new ParseField("regression");
    private static final MlConfigVersion MIN_SUPPORTED_VERSION = MlConfigVersion.V_7_6_0;
    private static final TransportVersion MIN_SUPPORTED_TRANSPORT_VERSION = TransportVersions.V_7_6_0;
    public static final ParseField NUM_TOP_FEATURE_IMPORTANCE_VALUES = new ParseField("num_top_feature_importance_values");

    public static RegressionConfig EMPTY_PARAMS = new RegressionConfig(DEFAULT_RESULTS_FIELD, null);

    private static final ObjectParser<RegressionConfig.Builder, Void> LENIENT_PARSER = createParser(true);
    private static final ObjectParser<RegressionConfig.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<RegressionConfig.Builder, Void> createParser(boolean lenient) {
        ObjectParser<RegressionConfig.Builder, Void> parser = new ObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            RegressionConfig.Builder::new
        );
        parser.declareString(RegressionConfig.Builder::setResultsField, RESULTS_FIELD);
        parser.declareInt(RegressionConfig.Builder::setNumTopFeatureImportanceValues, NUM_TOP_FEATURE_IMPORTANCE_VALUES);
        return parser;
    }

    public static RegressionConfig fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    public static RegressionConfig fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null).build();
    }

    private final String resultsField;
    private final int numTopFeatureImportanceValues;

    public RegressionConfig(String resultsField) {
        this(resultsField, 0);
    }

    public RegressionConfig(String resultsField, Integer numTopFeatureImportanceValues) {
        this.resultsField = resultsField == null ? DEFAULT_RESULTS_FIELD : resultsField;
        if (numTopFeatureImportanceValues != null && numTopFeatureImportanceValues < 0) {
            throw new IllegalArgumentException(
                "[" + NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName() + "] must be greater than or equal to 0"
            );
        }
        this.numTopFeatureImportanceValues = numTopFeatureImportanceValues == null ? 0 : numTopFeatureImportanceValues;
    }

    public RegressionConfig(StreamInput in) throws IOException {
        this.resultsField = in.readString();
        this.numTopFeatureImportanceValues = in.readVInt();
    }

    public int getNumTopFeatureImportanceValues() {
        return numTopFeatureImportanceValues;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public boolean requestingImportance() {
        return numTopFeatureImportanceValues > 0;
    }

    @Override
    public boolean isAllocateOnly() {
        return false;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(resultsField);
        out.writeVInt(numTopFeatureImportanceValues);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        builder.field(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), numTopFeatureImportanceValues);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegressionConfig that = (RegressionConfig) o;
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
    public InferenceConfig apply(InferenceConfigUpdate update) {
        if (update instanceof RegressionConfigUpdate configUpdate) {
            RegressionConfig.Builder builder = new RegressionConfig.Builder(this);
            if (configUpdate.getResultsField() != null) {
                builder.setResultsField(configUpdate.getResultsField());
            }
            if (configUpdate.getNumTopFeatureImportanceValues() != null) {
                builder.setNumTopFeatureImportanceValues(configUpdate.getNumTopFeatureImportanceValues());
            }
            return builder.build();
        } else if (update instanceof ResultsFieldUpdate resultsFieldUpdate) {
            return new RegressionConfig.Builder(this).setResultsField(resultsFieldUpdate.getResultsField()).build();
        } else {
            throw incompatibleUpdateException(update.getName());
        }
    }

    @Override
    public MlConfigVersion getMinimalSupportedMlConfigVersion() {
        return requestingImportance() ? MlConfigVersion.V_7_7_0 : MIN_SUPPORTED_VERSION;
    }

    @Override
    public TransportVersion getMinimalSupportedTransportVersion() {
        return requestingImportance() ? TransportVersions.V_7_7_0 : MIN_SUPPORTED_TRANSPORT_VERSION;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String resultsField;
        private Integer numTopFeatureImportanceValues;

        Builder() {}

        Builder(RegressionConfig config) {
            this.resultsField = config.resultsField;
            this.numTopFeatureImportanceValues = config.numTopFeatureImportanceValues;
        }

        public Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public Builder setNumTopFeatureImportanceValues(Integer numTopFeatureImportanceValues) {
            this.numTopFeatureImportanceValues = numTopFeatureImportanceValues;
            return this;
        }

        public RegressionConfig build() {
            return new RegressionConfig(resultsField, numTopFeatureImportanceValues);
        }
    }
}
