/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
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
import java.util.Objects;
import java.util.Optional;

public class FillMaskConfig implements NlpConfig {

    public static final String NAME = "fill_mask";
    public static final int DEFAULT_NUM_RESULTS = 5;

    public static FillMaskConfig fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    public static FillMaskConfig fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null).build();
    }

    private static final ObjectParser<FillMaskConfig.Builder, Void> STRICT_PARSER = createParser(false);
    private static final ObjectParser<FillMaskConfig.Builder, Void> LENIENT_PARSER = createParser(true);

    private static ObjectParser<FillMaskConfig.Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<FillMaskConfig.Builder, Void> parser = new ObjectParser<>(NAME, ignoreUnknownFields, Builder::new);
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
        parser.declareInt(Builder::setNumTopClasses, NUM_TOP_CLASSES);
        parser.declareString(Builder::setResultsField, RESULTS_FIELD);
        return parser;
    }

    private final VocabularyConfig vocabularyConfig;
    private final Tokenization tokenization;
    private final int numTopClasses;
    private final String resultsField;

    public FillMaskConfig(
        @Nullable VocabularyConfig vocabularyConfig,
        @Nullable Tokenization tokenization,
        @Nullable Integer numTopClasses,
        @Nullable String resultsField
    ) {
        this.vocabularyConfig = Optional.ofNullable(vocabularyConfig)
            .orElse(new VocabularyConfig(InferenceIndexConstants.nativeDefinitionStore()));
        this.tokenization = tokenization == null ? Tokenization.createDefault() : tokenization;
        this.numTopClasses = numTopClasses == null ? DEFAULT_NUM_RESULTS : numTopClasses;
        this.resultsField = resultsField;
        if (this.tokenization.span != -1) {
            throw ExceptionsHelper.badRequestException(
                "[{}] does not support windowing long text sequences; configured span [{}]",
                NAME,
                this.tokenization.span
            );
        }
    }

    public FillMaskConfig(StreamInput in) throws IOException {
        vocabularyConfig = new VocabularyConfig(in);
        tokenization = in.readNamedWriteable(Tokenization.class);
        numTopClasses = in.readInt();
        resultsField = in.readOptionalString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VOCABULARY.getPreferredName(), vocabularyConfig, params);
        NamedXContentObjectHelper.writeNamedObject(builder, params, TOKENIZATION.getPreferredName(), tokenization);
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
    public void writeTo(StreamOutput out) throws IOException {
        vocabularyConfig.writeTo(out);
        out.writeNamedWriteable(tokenization);
        out.writeInt(numTopClasses);
        out.writeOptionalString(resultsField);
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
    public TransportVersion getMinimalSupportedTransportVersion() {
        return TransportVersion.V_8_0_0;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FillMaskConfig that = (FillMaskConfig) o;
        return Objects.equals(vocabularyConfig, that.vocabularyConfig)
            && Objects.equals(tokenization, that.tokenization)
            && Objects.equals(resultsField, that.resultsField)
            && numTopClasses == that.numTopClasses;
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocabularyConfig, tokenization, numTopClasses, resultsField);
    }

    @Override
    public VocabularyConfig getVocabularyConfig() {
        return vocabularyConfig;
    }

    @Override
    public Tokenization getTokenization() {
        return tokenization;
    }

    public int getNumTopClasses() {
        return numTopClasses;
    }

    @Override
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
        private int numTopClasses;
        private String resultsField;

        Builder() {}

        Builder(FillMaskConfig config) {
            this.vocabularyConfig = config.vocabularyConfig;
            this.tokenization = config.tokenization;
            this.numTopClasses = config.numTopClasses;
            this.resultsField = config.resultsField;
        }

        public FillMaskConfig.Builder setVocabularyConfig(VocabularyConfig vocabularyConfig) {
            this.vocabularyConfig = vocabularyConfig;
            return this;
        }

        public FillMaskConfig.Builder setTokenization(Tokenization tokenization) {
            this.tokenization = tokenization;
            return this;
        }

        public FillMaskConfig.Builder setNumTopClasses(Integer numTopClasses) {
            this.numTopClasses = numTopClasses;
            return this;
        }

        public FillMaskConfig.Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public FillMaskConfig build() {
            return new FillMaskConfig(vocabularyConfig, tokenization, numTopClasses, resultsField);
        }
    }
}
