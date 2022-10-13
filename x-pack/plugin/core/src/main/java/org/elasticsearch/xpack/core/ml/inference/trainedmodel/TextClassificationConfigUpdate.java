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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.CLASSIFICATION_LABELS;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.NUM_TOP_CLASSES;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.TOKENIZATION;

public class TextClassificationConfigUpdate extends NlpConfigUpdate implements NamedXContentObject {

    public static final String NAME = TextClassificationConfig.NAME;

    @SuppressWarnings("unchecked")
    public static TextClassificationConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        Integer numTopClasses = (Integer) options.remove(NUM_TOP_CLASSES.getPreferredName());
        String resultsField = (String) options.remove(RESULTS_FIELD.getPreferredName());
        List<String> classificationLabels = (List<String>) options.remove(CLASSIFICATION_LABELS.getPreferredName());
        TokenizationUpdate tokenizationUpdate = NlpConfigUpdate.tokenizationFromMap(options);

        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", options.keySet());
        }
        return new TextClassificationConfigUpdate(classificationLabels, numTopClasses, resultsField, tokenizationUpdate);
    }

    private static final ObjectParser<TextClassificationConfigUpdate.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<TextClassificationConfigUpdate.Builder, Void> createParser(boolean lenient) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(NAME, lenient, TextClassificationConfigUpdate.Builder::new);
        parser.declareStringArray(Builder::setClassificationLabels, CLASSIFICATION_LABELS);
        parser.declareString(Builder::setResultsField, RESULTS_FIELD);
        parser.declareInt(Builder::setNumTopClasses, NUM_TOP_CLASSES);
        parser.declareNamedObject(
            Builder::setTokenizationUpdate,
            (p, c, n) -> p.namedObject(TokenizationUpdate.class, n, lenient),
            TOKENIZATION
        );
        return parser;
    }

    public static TextClassificationConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    private final List<String> classificationLabels;
    private final Integer numTopClasses;
    private final String resultsField;

    public TextClassificationConfigUpdate(
        List<String> classificationLabels,
        Integer numTopClasses,
        String resultsField,
        TokenizationUpdate tokenizationUpdate
    ) {
        super(tokenizationUpdate);
        this.classificationLabels = classificationLabels;
        this.numTopClasses = numTopClasses;
        this.resultsField = resultsField;
    }

    public TextClassificationConfigUpdate(StreamInput in) throws IOException {
        super(in);
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
    public Version getMinimalSupportedVersion() {
        return Version.V_8_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
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
                getName()
            );
        }

        TextClassificationConfig classificationConfig = (TextClassificationConfig) originalConfig;
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

        if (tokenizationUpdate != null) {
            builder.setTokenization(tokenizationUpdate.apply(classificationConfig.getTokenization()));
        }

        return builder.build();
    }

    boolean isNoop(TextClassificationConfig originalConfig) {
        return (this.numTopClasses == null || this.numTopClasses == originalConfig.getNumTopClasses())
            && (this.classificationLabels == null)
            && (this.resultsField == null || this.resultsField.equals(originalConfig.getResultsField()))
            && super.isNoop();
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return config instanceof TextClassificationConfig;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    public Integer getNumTopClasses() {
        return numTopClasses;
    }

    public List<String> getClassificationLabels() {
        return classificationLabels;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder().setClassificationLabels(classificationLabels)
            .setNumTopClasses(numTopClasses)
            .setResultsField(resultsField)
            .setTokenizationUpdate(tokenizationUpdate);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (numTopClasses != null) {
            builder.field(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        }
        if (classificationLabels != null) {
            builder.field(CLASSIFICATION_LABELS.getPreferredName(), classificationLabels);
        }
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextClassificationConfigUpdate that = (TextClassificationConfigUpdate) o;
        return Objects.equals(classificationLabels, that.classificationLabels)
            && Objects.equals(numTopClasses, that.numTopClasses)
            && Objects.equals(resultsField, that.resultsField)
            && Objects.equals(tokenizationUpdate, that.tokenizationUpdate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classificationLabels, numTopClasses, resultsField, tokenizationUpdate);
    }

    public static class Builder
        implements
            InferenceConfigUpdate.Builder<TextClassificationConfigUpdate.Builder, TextClassificationConfigUpdate> {
        private List<String> classificationLabels;
        private Integer numTopClasses;
        private String resultsField;
        private TokenizationUpdate tokenizationUpdate;

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

        public TextClassificationConfigUpdate.Builder setTokenizationUpdate(TokenizationUpdate tokenizationUpdate) {
            this.tokenizationUpdate = tokenizationUpdate;
            return this;
        }

        @Override
        public TextClassificationConfigUpdate build() {
            return new TextClassificationConfigUpdate(
                this.classificationLabels,
                this.numTopClasses,
                this.resultsField,
                this.tokenizationUpdate
            );
        }
    }
}
