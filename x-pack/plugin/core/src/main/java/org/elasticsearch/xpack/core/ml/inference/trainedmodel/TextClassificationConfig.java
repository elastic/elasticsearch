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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TextClassificationConfig implements NlpConfig {

    public static final String NAME = "text_classification";

    public static TextClassificationConfig fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    public static TextClassificationConfig fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null).build();
    }

    private static final ObjectParser<TextClassificationConfig.Builder, Void> STRICT_PARSER = createParser(false);
    private static final ObjectParser<TextClassificationConfig.Builder, Void> LENIENT_PARSER = createParser(true);

    private static ObjectParser<TextClassificationConfig.Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<TextClassificationConfig.Builder, Void> parser = new ObjectParser<>(NAME, ignoreUnknownFields, Builder::new);

        parser.declareObject(Builder::setVocabularyConfig, (p, c) -> {
            if (ignoreUnknownFields == false) {
                throw ExceptionsHelper.badRequestException(
                    "illegal setting [{}] on inference model creation",
                    VOCABULARY.getPreferredName()
                );
            }
            return VocabularyConfig.fromXContentLenient(p);
        }, VOCABULARY);
        parser.declareNamedObject(
            Builder::setTokenization,
            (p, c, n) -> p.namedObject(Tokenization.class, n, ignoreUnknownFields),
            TOKENIZATION
        );
        parser.declareStringArray(Builder::setClassificationLabels, CLASSIFICATION_LABELS);
        parser.declareInt(Builder::setNumTopClasses, NUM_TOP_CLASSES);
        parser.declareString(Builder::setResultsField, RESULTS_FIELD);
        return parser;
    }

    private final VocabularyConfig vocabularyConfig;
    private final Tokenization tokenization;
    private final List<String> classificationLabels;
    private final int numTopClasses;
    private final String resultsField;

    public TextClassificationConfig(
        @Nullable VocabularyConfig vocabularyConfig,
        @Nullable Tokenization tokenization,
        List<String> classificationLabels,
        @Nullable Integer numTopClasses,
        @Nullable String resultsField
    ) {
        this.vocabularyConfig = Optional.ofNullable(vocabularyConfig)
            .orElse(new VocabularyConfig(InferenceIndexConstants.nativeDefinitionStore()));
        this.tokenization = tokenization == null ? Tokenization.createDefault() : tokenization;
        if (classificationLabels == null || classificationLabels.size() < 2) {
            throw ExceptionsHelper.badRequestException(
                "[{}] requires at least 2 [{}]; provided {}",
                NAME,
                CLASSIFICATION_LABELS,
                classificationLabels
            );
        }
        this.classificationLabels = classificationLabels;
        this.numTopClasses = Optional.ofNullable(numTopClasses).orElse(-1);
        this.resultsField = resultsField;
    }

    public TextClassificationConfig(StreamInput in) throws IOException {
        vocabularyConfig = new VocabularyConfig(in);
        tokenization = in.readNamedWriteable(Tokenization.class);
        classificationLabels = in.readStringList();
        numTopClasses = in.readInt();
        resultsField = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        vocabularyConfig.writeTo(out);
        out.writeNamedWriteable(tokenization);
        out.writeStringCollection(classificationLabels);
        out.writeInt(numTopClasses);
        out.writeOptionalString(resultsField);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VOCABULARY.getPreferredName(), vocabularyConfig, params);
        NamedXContentObjectHelper.writeNamedObject(builder, params, TOKENIZATION.getPreferredName(), tokenization);
        builder.field(CLASSIFICATION_LABELS.getPreferredName(), classificationLabels);
        builder.field(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean isTargetTypeSupported(TargetType targetType) {
        return false;
    }

    @Override
    public Version getMinimalSupportedNodeVersion() {
        return Version.V_8_0_0;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TextClassificationConfig that = (TextClassificationConfig) o;
        return Objects.equals(vocabularyConfig, that.vocabularyConfig)
            && Objects.equals(tokenization, that.tokenization)
            && Objects.equals(numTopClasses, that.numTopClasses)
            && Objects.equals(classificationLabels, that.classificationLabels)
            && Objects.equals(resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocabularyConfig, tokenization, classificationLabels, numTopClasses, resultsField);
    }

    @Override
    public VocabularyConfig getVocabularyConfig() {
        return vocabularyConfig;
    }

    @Override
    public Tokenization getTokenization() {
        return tokenization;
    }

    public List<String> getClassificationLabels() {
        return classificationLabels;
    }

    public int getNumTopClasses() {
        return numTopClasses;
    }

    public String getResultsField() {
        return resultsField;
    }

    @Override
    public boolean isAllocateOnly() {
        return true;
    }

    public static class Builder {
        private VocabularyConfig vocabularyConfig;
        private Tokenization tokenization;
        private List<String> classificationLabels;
        private int numTopClasses;
        private String resultsField;

        Builder() {}

        Builder(TextClassificationConfig config) {
            this.vocabularyConfig = config.vocabularyConfig;
            this.tokenization = config.tokenization;
            this.classificationLabels = config.classificationLabels;
            this.numTopClasses = config.numTopClasses;
            this.resultsField = config.resultsField;
        }

        public Builder setVocabularyConfig(VocabularyConfig vocabularyConfig) {
            this.vocabularyConfig = vocabularyConfig;
            return this;
        }

        public Builder setTokenization(Tokenization tokenization) {
            this.tokenization = tokenization;
            return this;
        }

        public Builder setClassificationLabels(List<String> classificationLabels) {
            this.classificationLabels = classificationLabels;
            return this;
        }

        public Builder setNumTopClasses(Integer numTopClasses) {
            this.numTopClasses = numTopClasses;
            return this;
        }

        public Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public TextClassificationConfig build() {
            return new TextClassificationConfig(vocabularyConfig, tokenization, classificationLabels, numTopClasses, resultsField);
        }
    }
}
