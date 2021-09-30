/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig.CLASSIFICATION_LABELS;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig.NUM_TOP_CLASSES;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig.RESULTS_FIELD;

public class TextClassificationConfigUpdate extends NlpConfigUpdate implements NamedXContentObject {

    public static final String NAME = TextClassificationConfig.NAME;

    @SuppressWarnings("unchecked")
    public static TextClassificationConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        Integer numTopClasses = (Integer)options.remove(NUM_TOP_CLASSES.getPreferredName());
        String resultsField = (String)options.remove(RESULTS_FIELD.getPreferredName());
        List<String> classificationLabels = (List<String>)options.remove(CLASSIFICATION_LABELS.getPreferredName());

        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", options.keySet());
        }
        return new TextClassificationConfigUpdate(classificationLabels, numTopClasses, resultsField);
    }

    private static final ObjectParser<TextClassificationConfigUpdate.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<TextClassificationConfigUpdate.Builder, Void> createParser(boolean lenient) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(
            NAME,
            lenient,
            TextClassificationConfigUpdate.Builder::new);
        parser.declareStringArray(Builder::setClassificationLabels, CLASSIFICATION_LABELS);
        parser.declareString(Builder::setResultsField, RESULTS_FIELD);
        parser.declareInt(Builder::setNumTopClasses, NUM_TOP_CLASSES);
        return parser;
    }

    public static TextClassificationConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    private final List<String> classificationLabels;
    private final Integer numTopClasses;
    private final String resultsField;

    public TextClassificationConfigUpdate(List<String> classificationLabels, Integer numTopClasses, String resultsField) {
        this.classificationLabels = classificationLabels;
        this.numTopClasses = numTopClasses;
        this.resultsField = resultsField;
    }

    public TextClassificationConfigUpdate(StreamInput in) throws IOException {
        classificationLabels = in.readOptionalStringList();
        numTopClasses = in.readOptionalVInt();
        resultsField = in.readOptionalString();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStringCollection(classificationLabels);
        out.writeOptionalVInt(numTopClasses);
        out.writeOptionalString(resultsField);
    }

    @Override
    public InferenceConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof TextClassificationConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a request of type [{}]",
                originalConfig.getName(),
                getName());
        }

        TextClassificationConfig classificationConfig = (TextClassificationConfig)originalConfig;
        if (isNoop(classificationConfig)) {
            return originalConfig;
        }

        TextClassificationConfig.Builder builder = new TextClassificationConfig.Builder(classificationConfig);
        if (numTopClasses != null) {
            builder.setNumTopClasses(numTopClasses);
        }
        if (classificationLabels != null) {
            if (classificationLabels.size() != classificationConfig.getClassificationLabels().size()) {
                throw ExceptionsHelper.badRequestException(
                    "The number of [{}] the model is defined with [{}] does not match the number in the update [{}]",
                    CLASSIFICATION_LABELS,
                    classificationConfig.getClassificationLabels().size(),
                    classificationLabels.size()
                );
            }
            builder.setClassificationLabels(classificationLabels);
        }
        if (resultsField != null) {
            builder.setResultsField(resultsField);
        }
        return builder.build();
    }

    boolean isNoop(TextClassificationConfig originalConfig) {
        return (this.numTopClasses == null || this.numTopClasses == originalConfig.getNumTopClasses()) &&
            (this.classificationLabels == null) &&
            (this.resultsField == null || this.resultsField.equals(originalConfig.getResultsField()));
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return config instanceof TextClassificationConfig;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder()
            .setClassificationLabels(classificationLabels)
            .setNumTopClasses(numTopClasses)
            .setResultsField(resultsField);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (numTopClasses != null) {
            builder.field(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        }
        if (classificationLabels != null) {
            builder.field(CLASSIFICATION_LABELS.getPreferredName(), classificationLabels);
        }
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextClassificationConfigUpdate that = (TextClassificationConfigUpdate) o;
        return Objects.equals(classificationLabels, that.classificationLabels) &&
            Objects.equals(numTopClasses, that.numTopClasses) &&
            Objects.equals(resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classificationLabels, numTopClasses, resultsField);
    }

    public static class Builder
        implements InferenceConfigUpdate.Builder<TextClassificationConfigUpdate.Builder, TextClassificationConfigUpdate> {
        private List<String> classificationLabels;
        private Integer numTopClasses;
        private String resultsField;

        public TextClassificationConfigUpdate.Builder setNumTopClasses(Integer numTopClasses) {
            this.numTopClasses = numTopClasses;
            return this;
        }

        public TextClassificationConfigUpdate.Builder setClassificationLabels(List<String> classificationLabels) {
            this.classificationLabels = classificationLabels;
            return this;
        }

        @Override
        public TextClassificationConfigUpdate.Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public TextClassificationConfigUpdate build() {
            return new TextClassificationConfigUpdate(this.classificationLabels, this.numTopClasses, this.resultsField);
        }
    }
}
