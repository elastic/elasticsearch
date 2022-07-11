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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Sequence similarity configuration
 */
public class SequenceSimilarityConfig implements NlpConfig {

    public static final String NAME = "sequence_similarity";
    public static final ParseField SEQUENCE = new ParseField("sequence");
    public static final ParseField SPAN_SCORE_COMBINATION_FUNCTION = new ParseField("span_score_combination_function");

    public static SequenceSimilarityConfig fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static SequenceSimilarityConfig fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private static final ConstructingObjectParser<SequenceSimilarityConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<SequenceSimilarityConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<SequenceSimilarityConfig, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<SequenceSimilarityConfig, Void> parser = new ConstructingObjectParser<>(
            NAME,
            ignoreUnknownFields,
            a -> new SequenceSimilarityConfig((VocabularyConfig) a[0], (Tokenization) a[1], (String) a[2], (String) a[3])
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
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), SPAN_SCORE_COMBINATION_FUNCTION);
        return parser;
    }

    private final VocabularyConfig vocabularyConfig;
    private final Tokenization tokenization;
    private final String resultsField;
    private final String sequence;
    private final SpanScoreFunction spanScoreFunction;

    public SequenceSimilarityConfig(
        @Nullable VocabularyConfig vocabularyConfig,
        @Nullable Tokenization tokenization,
        @Nullable String resultsField,
        @Nullable String spanScoreFunction
    ) {
        this.vocabularyConfig = Optional.ofNullable(vocabularyConfig)
            .orElse(new VocabularyConfig(InferenceIndexConstants.nativeDefinitionStore()));
        this.tokenization = tokenization == null ? Tokenization.createDefault() : tokenization;
        this.resultsField = resultsField;
        this.sequence = null;
        this.spanScoreFunction = Optional.ofNullable(spanScoreFunction).map(SpanScoreFunction::fromString).orElse(SpanScoreFunction.MAX);
    }

    public SequenceSimilarityConfig(
        String sequence,
        VocabularyConfig vocabularyConfig,
        Tokenization tokenization,
        String resultsField,
        SpanScoreFunction spanScoreFunction
    ) {
        this.sequence = ExceptionsHelper.requireNonNull(sequence, SEQUENCE);
        this.vocabularyConfig = ExceptionsHelper.requireNonNull(vocabularyConfig, VOCABULARY);
        this.tokenization = ExceptionsHelper.requireNonNull(tokenization, TOKENIZATION);
        this.resultsField = resultsField;
        this.spanScoreFunction = spanScoreFunction;
    }

    public SequenceSimilarityConfig(StreamInput in) throws IOException {
        vocabularyConfig = new VocabularyConfig(in);
        tokenization = in.readNamedWriteable(Tokenization.class);
        resultsField = in.readOptionalString();
        sequence = in.readOptionalString();
        spanScoreFunction = in.readEnum(SpanScoreFunction.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        vocabularyConfig.writeTo(out);
        out.writeNamedWriteable(tokenization);
        out.writeOptionalString(resultsField);
        out.writeOptionalString(sequence);
        out.writeEnum(spanScoreFunction);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VOCABULARY.getPreferredName(), vocabularyConfig, params);
        NamedXContentObjectHelper.writeNamedObject(builder, params, TOKENIZATION.getPreferredName(), tokenization);
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        if (sequence != null) {
            builder.field(SEQUENCE.getPreferredName(), sequence);
        }
        builder.field(SPAN_SCORE_COMBINATION_FUNCTION.getPreferredName(), spanScoreFunction.toString());
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
    public Version getMinimalSupportedVersion() {
        return Version.V_8_4_0;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SequenceSimilarityConfig that = (SequenceSimilarityConfig) o;
        return Objects.equals(vocabularyConfig, that.vocabularyConfig)
            && Objects.equals(tokenization, that.tokenization)
            && Objects.equals(sequence, that.sequence)
            && Objects.equals(spanScoreFunction, that.spanScoreFunction)
            && Objects.equals(resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocabularyConfig, tokenization, resultsField, sequence, spanScoreFunction);
    }

    @Override
    public VocabularyConfig getVocabularyConfig() {
        return vocabularyConfig;
    }

    @Override
    public Tokenization getTokenization() {
        return tokenization;
    }

    public String getSequence() {
        return sequence;
    }

    public SpanScoreFunction getSpanScoreFunction() {
        return spanScoreFunction;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public boolean isAllocateOnly() {
        return true;
    }

    public enum SpanScoreFunction {
        MAX,
        MEAN;

        public static SpanScoreFunction fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

}
