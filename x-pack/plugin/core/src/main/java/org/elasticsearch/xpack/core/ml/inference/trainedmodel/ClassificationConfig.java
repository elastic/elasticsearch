/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;

import java.io.IOException;
import java.util.Objects;

public class ClassificationConfig implements LenientlyParsedInferenceConfig, StrictlyParsedInferenceConfig {

    public static final ParseField NAME = new ParseField("classification");

    public static final ParseField RESULTS_FIELD = new ParseField("results_field");
    public static final ParseField NUM_TOP_CLASSES = new ParseField("num_top_classes");
    public static final ParseField TOP_CLASSES_RESULTS_FIELD = new ParseField("top_classes_results_field");
    public static final ParseField NUM_TOP_FEATURE_IMPORTANCE_VALUES = new ParseField("num_top_feature_importance_values");
    public static final ParseField PREDICTION_FIELD_TYPE = new ParseField("prediction_field_type");
    private static final MlConfigVersion MIN_SUPPORTED_VERSION = MlConfigVersion.V_7_6_0;
    private static final TransportVersion MIN_SUPPORTED_TRANSPORT_VERSION = TransportVersion.V_7_6_0;

    public static ClassificationConfig EMPTY_PARAMS = new ClassificationConfig(
        0,
        DEFAULT_RESULTS_FIELD,
        DEFAULT_TOP_CLASSES_RESULTS_FIELD,
        null,
        null
    );

    private final int numTopClasses;
    private final String topClassesResultsField;
    private final String resultsField;
    private final int numTopFeatureImportanceValues;
    private final PredictionFieldType predictionFieldType;

    private static final ObjectParser<ClassificationConfig.Builder, Void> LENIENT_PARSER = createParser(true);
    private static final ObjectParser<ClassificationConfig.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<ClassificationConfig.Builder, Void> createParser(boolean lenient) {
        ObjectParser<ClassificationConfig.Builder, Void> parser = new ObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            ClassificationConfig.Builder::new
        );
        parser.declareInt(ClassificationConfig.Builder::setNumTopClasses, NUM_TOP_CLASSES);
        parser.declareString(ClassificationConfig.Builder::setResultsField, RESULTS_FIELD);
        parser.declareString(ClassificationConfig.Builder::setTopClassesResultsField, TOP_CLASSES_RESULTS_FIELD);
        parser.declareInt(ClassificationConfig.Builder::setNumTopFeatureImportanceValues, NUM_TOP_FEATURE_IMPORTANCE_VALUES);
        parser.declareField(ClassificationConfig.Builder::setPredictionFieldType, (p, c) -> {
            try {
                return PredictionFieldType.fromString(p.text());
            } catch (IllegalArgumentException iae) {
                if (lenient) {
                    return PredictionFieldType.STRING;
                }
                throw iae;
            }
        }, PREDICTION_FIELD_TYPE, ObjectParser.ValueType.STRING);
        return parser;
    }

    public static ClassificationConfig fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    public static ClassificationConfig fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null).build();
    }

    public ClassificationConfig(Integer numTopClasses) {
        this(numTopClasses, null, null, null, null);
    }

    public ClassificationConfig(
        Integer numTopClasses,
        String resultsField,
        String topClassesResultsField,
        Integer featureImportance,
        PredictionFieldType predictionFieldType
    ) {
        this.numTopClasses = numTopClasses == null ? 0 : numTopClasses;
        this.topClassesResultsField = topClassesResultsField == null ? DEFAULT_TOP_CLASSES_RESULTS_FIELD : topClassesResultsField;
        this.resultsField = resultsField == null ? DEFAULT_RESULTS_FIELD : resultsField;
        if (featureImportance != null && featureImportance < 0) {
            throw new IllegalArgumentException(
                "[" + NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName() + "] must be greater than or equal to 0"
            );
        }
        this.numTopFeatureImportanceValues = featureImportance == null ? 0 : featureImportance;
        this.predictionFieldType = predictionFieldType == null ? PredictionFieldType.STRING : predictionFieldType;
    }

    public ClassificationConfig(StreamInput in) throws IOException {
        this.numTopClasses = in.readInt();
        this.topClassesResultsField = in.readString();
        this.resultsField = in.readString();
        this.numTopFeatureImportanceValues = in.readVInt();
        this.predictionFieldType = PredictionFieldType.fromStream(in);
    }

    public int getNumTopClasses() {
        return numTopClasses;
    }

    public String getTopClassesResultsField() {
        return topClassesResultsField;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    public int getNumTopFeatureImportanceValues() {
        return numTopFeatureImportanceValues;
    }

    public PredictionFieldType getPredictionFieldType() {
        return predictionFieldType;
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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(numTopClasses);
        out.writeString(topClassesResultsField);
        out.writeString(resultsField);
        out.writeVInt(numTopFeatureImportanceValues);
        predictionFieldType.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClassificationConfig that = (ClassificationConfig) o;
        return Objects.equals(numTopClasses, that.numTopClasses)
            && Objects.equals(topClassesResultsField, that.topClassesResultsField)
            && Objects.equals(resultsField, that.resultsField)
            && Objects.equals(numTopFeatureImportanceValues, that.numTopFeatureImportanceValues)
            && Objects.equals(predictionFieldType, that.predictionFieldType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numTopClasses, topClassesResultsField, resultsField, numTopFeatureImportanceValues, predictionFieldType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        builder.field(TOP_CLASSES_RESULTS_FIELD.getPreferredName(), topClassesResultsField);
        builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        builder.field(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), numTopFeatureImportanceValues);
        builder.field(PREDICTION_FIELD_TYPE.getPreferredName(), predictionFieldType.toString());
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
    public MlConfigVersion getMinimalSupportedMlConfigVersion() {
        return requestingImportance() ? MlConfigVersion.V_7_7_0 : MIN_SUPPORTED_VERSION;
    }

    @Override
    public TransportVersion getMinimalSupportedTransportVersion() {
        return requestingImportance() ? TransportVersion.V_7_7_0 : MIN_SUPPORTED_TRANSPORT_VERSION;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Integer numTopClasses;
        private String topClassesResultsField;
        private String resultsField;
        private PredictionFieldType predictionFieldType;
        private Integer numTopFeatureImportanceValues;

        Builder() {}

        Builder(ClassificationConfig config) {
            this.numTopClasses = config.numTopClasses;
            this.topClassesResultsField = config.topClassesResultsField;
            this.resultsField = config.resultsField;
            this.numTopFeatureImportanceValues = config.numTopFeatureImportanceValues;
            this.predictionFieldType = config.predictionFieldType;
        }

        public Builder setNumTopClasses(Integer numTopClasses) {
            this.numTopClasses = numTopClasses;
            return this;
        }

        public Builder setTopClassesResultsField(String topClassesResultsField) {
            this.topClassesResultsField = topClassesResultsField;
            return this;
        }

        public Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public Builder setNumTopFeatureImportanceValues(Integer numTopFeatureImportanceValues) {
            this.numTopFeatureImportanceValues = numTopFeatureImportanceValues;
            return this;
        }

        public Builder setPredictionFieldType(PredictionFieldType predictionFieldType) {
            this.predictionFieldType = predictionFieldType;
            return this;
        }

        public ClassificationConfig build() {
            return new ClassificationConfig(
                numTopClasses,
                resultsField,
                topClassesResultsField,
                numTopFeatureImportanceValues,
                predictionFieldType
            );
        }
    }
}
