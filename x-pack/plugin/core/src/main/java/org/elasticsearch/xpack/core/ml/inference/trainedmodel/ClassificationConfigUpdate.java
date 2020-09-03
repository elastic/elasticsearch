/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig.NUM_TOP_CLASSES;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig.NUM_TOP_FEATURE_IMPORTANCE_VALUES;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig.PREDICTION_FIELD_TYPE;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig.RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig.TOP_CLASSES_RESULTS_FIELD;

public class ClassificationConfigUpdate implements InferenceConfigUpdate, NamedXContentObject {

    public static final ParseField NAME = ClassificationConfig.NAME;

    public static ClassificationConfigUpdate EMPTY_PARAMS =
        new ClassificationConfigUpdate(null, null, null, null, null);

    private final Integer numTopClasses;
    private final String topClassesResultsField;
    private final String resultsField;
    private final Integer numTopFeatureImportanceValues;
    private final PredictionFieldType predictionFieldType;

    public static ClassificationConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        Integer numTopClasses = (Integer)options.remove(NUM_TOP_CLASSES.getPreferredName());
        String topClassesResultsField = (String)options.remove(TOP_CLASSES_RESULTS_FIELD.getPreferredName());
        String resultsField = (String)options.remove(RESULTS_FIELD.getPreferredName());
        Integer featureImportance = (Integer)options.remove(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName());
        String predictionFieldTypeStr = (String)options.remove(PREDICTION_FIELD_TYPE.getPreferredName());

        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", options.keySet());
        }
        return new ClassificationConfigUpdate(numTopClasses,
            resultsField,
            topClassesResultsField,
            featureImportance,
            predictionFieldTypeStr == null ? null : PredictionFieldType.fromString(predictionFieldTypeStr));
    }

    public static ClassificationConfigUpdate fromConfig(ClassificationConfig config) {
        return new ClassificationConfigUpdate(config.getNumTopClasses(),
            config.getResultsField(),
            config.getTopClassesResultsField(),
            config.getNumTopFeatureImportanceValues(),
            config.getPredictionFieldType());
    }

    private static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<Builder, Void> createParser(boolean lenient) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            Builder::new);
        parser.declareInt(Builder::setNumTopClasses, NUM_TOP_CLASSES);
        parser.declareString(Builder::setResultsField, RESULTS_FIELD);
        parser.declareString(Builder::setTopClassesResultsField, TOP_CLASSES_RESULTS_FIELD);
        parser.declareInt(Builder::setNumTopFeatureImportanceValues, NUM_TOP_FEATURE_IMPORTANCE_VALUES);
        parser.declareString(Builder::setPredictionFieldType, PREDICTION_FIELD_TYPE);
        return parser;
    }

    public static ClassificationConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    public ClassificationConfigUpdate(Integer numTopClasses,
                                      String resultsField,
                                      String topClassesResultsField,
                                      Integer featureImportance,
                                      PredictionFieldType predictionFieldType) {
        this.numTopClasses = numTopClasses;
        this.topClassesResultsField = topClassesResultsField;
        this.resultsField = resultsField;
        if (featureImportance != null && featureImportance < 0) {
            throw new IllegalArgumentException("[" + NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName() +
                "] must be greater than or equal to 0");
        }
        this.numTopFeatureImportanceValues = featureImportance;
        this.predictionFieldType = predictionFieldType;

        InferenceConfigUpdate.checkFieldUniqueness(resultsField, topClassesResultsField);
    }

    public ClassificationConfigUpdate(StreamInput in) throws IOException {
        this.numTopClasses = in.readOptionalInt();
        this.topClassesResultsField = in.readOptionalString();
        this.resultsField = in.readOptionalString();
        this.numTopFeatureImportanceValues = in.readOptionalVInt();
        this.predictionFieldType = in.readOptionalWriteable(PredictionFieldType::fromStream);
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

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder()
            .setNumTopClasses(numTopClasses)
            .setTopClassesResultsField(topClassesResultsField)
            .setResultsField(resultsField)
            .setNumTopFeatureImportanceValues(numTopFeatureImportanceValues)
            .setPredictionFieldType(predictionFieldType);
    }

    public Integer getNumTopFeatureImportanceValues() {
        return numTopFeatureImportanceValues;
    }

    public PredictionFieldType getPredictionFieldType() {
        return predictionFieldType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(numTopClasses);
        out.writeOptionalString(topClassesResultsField);
        out.writeOptionalString(resultsField);
        out.writeOptionalVInt(numTopFeatureImportanceValues);
        out.writeOptionalWriteable(predictionFieldType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClassificationConfigUpdate that = (ClassificationConfigUpdate) o;
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
        if (predictionFieldType != null) {
            builder.field(PREDICTION_FIELD_TYPE.getPreferredName(), predictionFieldType.toString());
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
    public InferenceConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof ClassificationConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a inference request of type [{}]",
                originalConfig.getName(),
                getName());
        }
        ClassificationConfig classificationConfig = (ClassificationConfig)originalConfig;

        if (isNoop(classificationConfig)) {
            return originalConfig;
        }
        ClassificationConfig.Builder builder = new ClassificationConfig.Builder(classificationConfig);
        if (resultsField != null) {
            builder.setResultsField(resultsField);
        }
        if (numTopFeatureImportanceValues != null) {
            builder.setNumTopFeatureImportanceValues(numTopFeatureImportanceValues);
        }
        if (topClassesResultsField != null) {
            builder.setTopClassesResultsField(topClassesResultsField);
        }
        if (numTopClasses != null) {
            builder.setNumTopClasses(numTopClasses);
        }
        if (predictionFieldType != null) {
            builder.setPredictionFieldType(predictionFieldType);
        }
        return builder.build();
    }

    @Override
    public InferenceConfig toConfig() {
        return apply(ClassificationConfig.EMPTY_PARAMS);
    }

    @Override
    public boolean isSupported(InferenceConfig inferenceConfig) {
        return inferenceConfig instanceof ClassificationConfig;
    }

    boolean isNoop(ClassificationConfig originalConfig) {
        return (resultsField == null || resultsField.equals(originalConfig.getResultsField()))
            && (numTopFeatureImportanceValues == null
                || originalConfig.getNumTopFeatureImportanceValues() == numTopFeatureImportanceValues)
            && (topClassesResultsField == null || topClassesResultsField.equals(originalConfig.getTopClassesResultsField()))
            && (numTopClasses == null || originalConfig.getNumTopClasses() == numTopClasses)
            && (predictionFieldType == null || predictionFieldType.equals(originalConfig.getPredictionFieldType()));
    }

    public static class Builder implements InferenceConfigUpdate.Builder<Builder, ClassificationConfigUpdate> {
        private Integer numTopClasses;
        private String topClassesResultsField;
        private String resultsField;
        private Integer numTopFeatureImportanceValues;
        private PredictionFieldType predictionFieldType;

        public Builder setNumTopClasses(Integer numTopClasses) {
            this.numTopClasses = numTopClasses;
            return this;
        }

        public Builder setTopClassesResultsField(String topClassesResultsField) {
            this.topClassesResultsField = topClassesResultsField;
            return this;
        }

        @Override
        public Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public Builder setNumTopFeatureImportanceValues(Integer numTopFeatureImportanceValues) {
            this.numTopFeatureImportanceValues = numTopFeatureImportanceValues;
            return this;
        }

        public Builder setPredictionFieldType(PredictionFieldType predictionFieldtype) {
            this.predictionFieldType = predictionFieldtype;
            return this;
        }

        private Builder setPredictionFieldType(String predictionFieldType) {
            return setPredictionFieldType(PredictionFieldType.fromString(predictionFieldType));
        }

        @Override
        public ClassificationConfigUpdate build() {
            return new ClassificationConfigUpdate(numTopClasses,
                resultsField,
                topClassesResultsField,
                numTopFeatureImportanceValues,
                predictionFieldType);
        }
    }
}
