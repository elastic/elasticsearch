/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig.NUM_TOP_FEATURE_IMPORTANCE_VALUES;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig.RESULTS_FIELD;

public class RegressionConfigUpdate implements InferenceConfigUpdate, NamedXContentObject {

    public static final ParseField NAME = RegressionConfig.NAME;

    public static RegressionConfigUpdate EMPTY_PARAMS = new RegressionConfigUpdate(null, null);

    public static RegressionConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        String resultsField = (String) options.remove(RESULTS_FIELD.getPreferredName());
        Integer featureImportance = (Integer) options.remove(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName());
        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", map.keySet());
        }
        return new RegressionConfigUpdate(resultsField, featureImportance);
    }

    public static RegressionConfigUpdate fromConfig(RegressionConfig config) {
        return new RegressionConfigUpdate(config.getResultsField(), config.getNumTopFeatureImportanceValues());
    }

    private static final ObjectParser<RegressionConfigUpdate.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<RegressionConfigUpdate.Builder, Void> createParser(boolean lenient) {
        ObjectParser<RegressionConfigUpdate.Builder, Void> parser = new ObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            RegressionConfigUpdate.Builder::new
        );
        parser.declareString(RegressionConfigUpdate.Builder::setResultsField, RESULTS_FIELD);
        parser.declareInt(RegressionConfigUpdate.Builder::setNumTopFeatureImportanceValues, NUM_TOP_FEATURE_IMPORTANCE_VALUES);
        return parser;
    }

    public static RegressionConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    private final String resultsField;
    private final Integer numTopFeatureImportanceValues;

    public RegressionConfigUpdate(String resultsField, Integer numTopFeatureImportanceValues) {
        this.resultsField = resultsField;
        if (numTopFeatureImportanceValues != null && numTopFeatureImportanceValues < 0) {
            throw new IllegalArgumentException(
                "[" + NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName() + "] must be greater than or equal to 0"
            );
        }
        this.numTopFeatureImportanceValues = numTopFeatureImportanceValues;

        InferenceConfigUpdate.checkFieldUniqueness(resultsField);
    }

    public RegressionConfigUpdate(StreamInput in) throws IOException {
        this.resultsField = in.readOptionalString();
        this.numTopFeatureImportanceValues = in.readOptionalVInt();
    }

    public Integer getNumTopFeatureImportanceValues() {
        return numTopFeatureImportanceValues;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder().setNumTopFeatureImportanceValues(numTopFeatureImportanceValues).setResultsField(resultsField);
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(resultsField);
        out.writeOptionalVInt(numTopFeatureImportanceValues);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_8_0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegressionConfigUpdate that = (RegressionConfigUpdate) o;
        return Objects.equals(this.resultsField, that.resultsField)
            && Objects.equals(this.numTopFeatureImportanceValues, that.numTopFeatureImportanceValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultsField, numTopFeatureImportanceValues);
    }

    @Override
    public InferenceConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof RegressionConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a inference request of type [{}]",
                originalConfig.getName(),
                getName()
            );
        }

        RegressionConfig regressionConfig = (RegressionConfig) originalConfig;
        if (isNoop(regressionConfig)) {
            return originalConfig;
        }
        RegressionConfig.Builder builder = new RegressionConfig.Builder(regressionConfig);
        if (resultsField != null) {
            builder.setResultsField(resultsField);
        }
        if (numTopFeatureImportanceValues != null) {
            builder.setNumTopFeatureImportanceValues(numTopFeatureImportanceValues);
        }
        return builder.build();
    }

    @Override
    public boolean isSupported(InferenceConfig inferenceConfig) {
        return inferenceConfig instanceof RegressionConfig;
    }

    boolean isNoop(RegressionConfig originalConfig) {
        return (resultsField == null || originalConfig.getResultsField().equals(resultsField))
            && (numTopFeatureImportanceValues == null
                || originalConfig.getNumTopFeatureImportanceValues() == numTopFeatureImportanceValues);
    }

    public static class Builder implements InferenceConfigUpdate.Builder<Builder, RegressionConfigUpdate> {
        private String resultsField;
        private Integer numTopFeatureImportanceValues;

        @Override
        public Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public Builder setNumTopFeatureImportanceValues(Integer numTopFeatureImportanceValues) {
            this.numTopFeatureImportanceValues = numTopFeatureImportanceValues;
            return this;
        }

        @Override
        public RegressionConfigUpdate build() {
            return new RegressionConfigUpdate(resultsField, numTopFeatureImportanceValues);
        }
    }
}
