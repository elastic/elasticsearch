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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Question and Answer configuration
 */
public class QuestionAnsweringConfig implements NlpConfig {

    public static final String NAME = "question_answering";
    public static final ParseField MAX_ANSWER_LENGTH = new ParseField("max_answer_length");
    public static final ParseField QUESTION = new ParseField("question");

    public static QuestionAnsweringConfig fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static QuestionAnsweringConfig fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    public static final int DEFAULT_MAX_ANSWER_LENGTH = 15;
    public static final int DEFAULT_NUM_TOP_CLASSES = 0;

    private static final ConstructingObjectParser<QuestionAnsweringConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<QuestionAnsweringConfig, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings({ "unchecked" })
    private static ConstructingObjectParser<QuestionAnsweringConfig, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<QuestionAnsweringConfig, Void> parser = new ConstructingObjectParser<>(
            NAME,
            ignoreUnknownFields,
            a -> new QuestionAnsweringConfig((Integer) a[0], (Integer) a[1], (VocabularyConfig) a[2], (Tokenization) a[3], (String) a[4])
        );
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), NUM_TOP_CLASSES);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_ANSWER_LENGTH);
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

    private final int numTopClasses;
    private final int maxAnswerLength;
    private final VocabularyConfig vocabularyConfig;
    private final Tokenization tokenization;
    private final String resultsField;
    private final String question;

    public QuestionAnsweringConfig(
        @Nullable Integer numTopClasses,
        @Nullable Integer maxAnswerLength,
        @Nullable VocabularyConfig vocabularyConfig,
        @Nullable Tokenization tokenization,
        @Nullable String resultsField
    ) {
        this.numTopClasses = Optional.ofNullable(numTopClasses).orElse(DEFAULT_NUM_TOP_CLASSES);
        this.maxAnswerLength = Optional.ofNullable(maxAnswerLength).orElse(DEFAULT_MAX_ANSWER_LENGTH);
        if (this.numTopClasses < 0) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be greater than or equal to [0]; provided [{}]",
                NUM_TOP_CLASSES.getPreferredName(),
                this.numTopClasses
            );
        }
        if (this.maxAnswerLength <= 0) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be greater than [0]; provided [{}]",
                MAX_ANSWER_LENGTH.getPreferredName(),
                this.maxAnswerLength
            );
        }
        this.vocabularyConfig = Optional.ofNullable(vocabularyConfig)
            .orElse(new VocabularyConfig(InferenceIndexConstants.nativeDefinitionStore()));
        this.tokenization = tokenization == null ? Tokenization.createDefault() : tokenization;
        this.resultsField = resultsField;
        this.question = null;
    }

    public QuestionAnsweringConfig(
        String question,
        int numTopClasses,
        int maxAnswerLength,
        VocabularyConfig vocabularyConfig,
        Tokenization tokenization,
        String resultsField
    ) {
        this.question = ExceptionsHelper.requireNonNull(question, QUESTION);
        this.numTopClasses = numTopClasses;
        this.maxAnswerLength = maxAnswerLength;
        if (this.numTopClasses < 0) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be greater than or equal to [0]; provided [{}]",
                NUM_TOP_CLASSES.getPreferredName(),
                this.numTopClasses
            );
        }
        if (this.maxAnswerLength <= 0) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be greater than [0]; provided [{}]",
                MAX_ANSWER_LENGTH.getPreferredName(),
                this.maxAnswerLength
            );
        }
        this.vocabularyConfig = ExceptionsHelper.requireNonNull(vocabularyConfig, VOCABULARY);
        this.tokenization = ExceptionsHelper.requireNonNull(tokenization, TOKENIZATION);
        this.resultsField = resultsField;
    }

    public QuestionAnsweringConfig(StreamInput in) throws IOException {
        numTopClasses = in.readVInt();
        maxAnswerLength = in.readVInt();
        vocabularyConfig = new VocabularyConfig(in);
        tokenization = in.readNamedWriteable(Tokenization.class);
        resultsField = in.readOptionalString();
        question = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numTopClasses);
        out.writeVInt(maxAnswerLength);
        vocabularyConfig.writeTo(out);
        out.writeNamedWriteable(tokenization);
        out.writeOptionalString(resultsField);
        out.writeOptionalString(question);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        builder.field(MAX_ANSWER_LENGTH.getPreferredName(), maxAnswerLength);
        builder.field(VOCABULARY.getPreferredName(), vocabularyConfig, params);
        NamedXContentObjectHelper.writeNamedObject(builder, params, TOKENIZATION.getPreferredName(), tokenization);
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        if (question != null) {
            builder.field(QUESTION.getPreferredName(), question);
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
        return Version.V_8_3_0;
    }

    @Override
    public TransportVersion getMinimalSupportedTransportVersion() {
        return TransportVersion.V_8_3_0;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QuestionAnsweringConfig that = (QuestionAnsweringConfig) o;
        return Objects.equals(vocabularyConfig, that.vocabularyConfig)
            && Objects.equals(tokenization, that.tokenization)
            && Objects.equals(numTopClasses, that.numTopClasses)
            && Objects.equals(maxAnswerLength, that.maxAnswerLength)
            && Objects.equals(question, that.question)
            && Objects.equals(resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocabularyConfig, tokenization, maxAnswerLength, numTopClasses, resultsField, question);
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

    public int getMaxAnswerLength() {
        return maxAnswerLength;
    }

    public String getQuestion() {
        return question;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public boolean isAllocateOnly() {
        return true;
    }

}
