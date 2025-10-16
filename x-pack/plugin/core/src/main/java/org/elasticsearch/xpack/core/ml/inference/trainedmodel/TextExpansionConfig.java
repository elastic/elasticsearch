/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class TextExpansionConfig implements NlpConfig {

    public static final String NAME = "text_expansion";

    public static TextExpansionConfig fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static TextExpansionConfig fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private static final ConstructingObjectParser<TextExpansionConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<TextExpansionConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<TextExpansionConfig, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<TextExpansionConfig, Void> parser = new ConstructingObjectParser<>(
            NAME,
            ignoreUnknownFields,
            a -> new TextExpansionConfig((VocabularyConfig) a[0], (Tokenization) a[1], (String) a[2])
        );
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            if (ignoreUnknownFields == false) {
                throw ExceptionsHelper.badRequestException(
                    "illegal setting [{}] on inference model creation",
                    VOCABULARY.getPreferredName()
                );
            }
            return VocabularyConfig.fromXContentLenient(p);
        }, VOCABULARY);
        parser.declareNamedObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c, n) -> p.namedObject(Tokenization.class, n, ignoreUnknownFields),
            TOKENIZATION
        );
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), RESULTS_FIELD);
        return parser;
    }

    private final VocabularyConfig vocabularyConfig;
    private final Tokenization tokenization;
    private final String resultsField;

    public TextExpansionConfig(
        @Nullable VocabularyConfig vocabularyConfig,
        @Nullable Tokenization tokenization,
        @Nullable String resultsField
    ) {
        this.vocabularyConfig = Optional.ofNullable(vocabularyConfig)
            .orElse(new VocabularyConfig(InferenceIndexConstants.nativeDefinitionStore()));
        this.tokenization = tokenization == null ? Tokenization.createDefault() : tokenization;
        this.resultsField = resultsField;
    }

    public TextExpansionConfig(StreamInput in) throws IOException {
        vocabularyConfig = new VocabularyConfig(in);
        tokenization = in.readNamedWriteable(Tokenization.class);
        resultsField = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        vocabularyConfig.writeTo(out);
        out.writeNamedWriteable(tokenization);
        out.writeOptionalString(resultsField);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VOCABULARY.getPreferredName(), vocabularyConfig, params);
        NamedXContentObjectHelper.writeNamedObject(builder, params, TOKENIZATION.getPreferredName(), tokenization);
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
        // TargetType relates to boosted tree models
        return false;
    }

    @Override
    public InferenceConfig apply(InferenceConfigUpdate update) {
        if (update instanceof TextExpansionConfigUpdate configUpdate) {
            return new TextExpansionConfig(
                vocabularyConfig,
                configUpdate.tokenizationUpdate == null ? tokenization : configUpdate.tokenizationUpdate.apply(tokenization),
                Optional.ofNullable(configUpdate.getResultsField()).orElse(resultsField)
            );
        } else if (update instanceof TokenizationConfigUpdate tokenizationUpdate) {
            var updatedTokenization = getTokenization().updateWindowSettings(tokenizationUpdate.getSpanSettings());
            return new TextExpansionConfig(vocabularyConfig, updatedTokenization, resultsField);
        } else {
            throw incompatibleUpdateException(update.getName());
        }
    }

    @Override
    public boolean isAllocateOnly() {
        return true;
    }

    @Override
    public MlConfigVersion getMinimalSupportedMlConfigVersion() {
        return MlConfigVersion.V_8_7_0;
    }

    @Override
    public TransportVersion getMinimalSupportedTransportVersion() {
        return TransportVersions.V_8_7_0;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public VocabularyConfig getVocabularyConfig() {
        return vocabularyConfig;
    }

    @Override
    public Tokenization getTokenization() {
        return tokenization;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextExpansionConfig that = (TextExpansionConfig) o;
        return Objects.equals(vocabularyConfig, that.vocabularyConfig)
            && Objects.equals(tokenization, that.tokenization)
            && Objects.equals(resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocabularyConfig, tokenization, resultsField);
    }
}
