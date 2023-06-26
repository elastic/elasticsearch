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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.NUM_TOP_CLASSES;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.TOKENIZATION;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfig.MAX_ANSWER_LENGTH;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfig.QUESTION;

public class QuestionAnsweringConfigUpdate extends NlpConfigUpdate implements NamedXContentObject {

    public static final String NAME = "question_answering";

    public static QuestionAnsweringConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    @SuppressWarnings({ "unchecked" })
    public static QuestionAnsweringConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        Integer numTopClasses = (Integer) options.remove(NUM_TOP_CLASSES.getPreferredName());
        Integer maxAnswerLength = (Integer) options.remove(MAX_ANSWER_LENGTH.getPreferredName());
        String resultsField = (String) options.remove(RESULTS_FIELD.getPreferredName());
        String question = (String) options.remove(QUESTION.getPreferredName());
        TokenizationUpdate tokenizationUpdate = NlpConfigUpdate.tokenizationFromMap(options);
        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", map.keySet());
        }
        return new QuestionAnsweringConfigUpdate(question, numTopClasses, maxAnswerLength, resultsField, tokenizationUpdate);
    }

    @SuppressWarnings({ "unchecked" })
    private static final ObjectParser<QuestionAnsweringConfigUpdate.Builder, Void> STRICT_PARSER = new ObjectParser<>(
        NAME,
        QuestionAnsweringConfigUpdate.Builder::new
    );

    static {
        STRICT_PARSER.declareString(Builder::setQuestion, QUESTION);
        STRICT_PARSER.declareInt(Builder::setNumTopClasses, NUM_TOP_CLASSES);
        STRICT_PARSER.declareInt(Builder::setMaxAnswerLength, MAX_ANSWER_LENGTH);
        STRICT_PARSER.declareString(Builder::setResultsField, RESULTS_FIELD);
        STRICT_PARSER.declareNamedObject(
            Builder::setTokenizationUpdate,
            (p, c, n) -> p.namedObject(TokenizationUpdate.class, n, false),
            TOKENIZATION
        );
    }

    private final String question;
    private final Integer numTopClasses;
    private final Integer maxAnswerLength;
    private final String resultsField;

    public QuestionAnsweringConfigUpdate(
        String question,
        @Nullable Integer numTopClasses,
        @Nullable Integer maxAnswerLength,
        @Nullable String resultsField,
        @Nullable TokenizationUpdate tokenizationUpdate
    ) {
        super(tokenizationUpdate);
        this.question = ExceptionsHelper.requireNonNull(question, QUESTION);
        this.numTopClasses = numTopClasses;
        this.maxAnswerLength = maxAnswerLength;
        this.resultsField = resultsField;
    }

    public QuestionAnsweringConfigUpdate(StreamInput in) throws IOException {
        super(in);
        question = in.readString();
        numTopClasses = in.readOptionalInt();
        maxAnswerLength = in.readOptionalInt();
        resultsField = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(question);
        out.writeOptionalInt(numTopClasses);
        out.writeOptionalInt(maxAnswerLength);
        out.writeOptionalString(resultsField);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (numTopClasses != null) {
            builder.field(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        }
        if (maxAnswerLength != null) {
            builder.field(MAX_ANSWER_LENGTH.getPreferredName(), maxAnswerLength);
        }
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        builder.field(QUESTION.getPreferredName(), question);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public InferenceConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof QuestionAnsweringConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a inference request of type [{}]",
                originalConfig.getName(),
                getName()
            );
        }

        QuestionAnsweringConfig questionAnsweringConfig = (QuestionAnsweringConfig) originalConfig;
        return new QuestionAnsweringConfig(
            question,
            Optional.ofNullable(numTopClasses).orElse(questionAnsweringConfig.getNumTopClasses()),
            Optional.ofNullable(maxAnswerLength).orElse(questionAnsweringConfig.getMaxAnswerLength()),
            questionAnsweringConfig.getVocabularyConfig(),
            tokenizationUpdate == null
                ? questionAnsweringConfig.getTokenization()
                : tokenizationUpdate.apply(questionAnsweringConfig.getTokenization()),
            Optional.ofNullable(resultsField).orElse(questionAnsweringConfig.getResultsField())
        );
    }

    boolean isNoop(QuestionAnsweringConfig originalConfig) {
        return (numTopClasses == null || numTopClasses.equals(originalConfig.getNumTopClasses()))
            && (maxAnswerLength == null || maxAnswerLength.equals(originalConfig.getMaxAnswerLength()))
            && (resultsField == null || resultsField.equals(originalConfig.getResultsField()))
            && (question == null || question.equals(originalConfig.getQuestion()))
            && super.isNoop();
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return config instanceof QuestionAnsweringConfig;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder().setQuestion(question)
            .setNumTopClasses(numTopClasses)
            .setMaxAnswerLength(maxAnswerLength)
            .setResultsField(resultsField)
            .setTokenizationUpdate(tokenizationUpdate);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QuestionAnsweringConfigUpdate that = (QuestionAnsweringConfigUpdate) o;
        return Objects.equals(numTopClasses, that.numTopClasses)
            && Objects.equals(maxAnswerLength, that.maxAnswerLength)
            && Objects.equals(question, that.question)
            && Objects.equals(resultsField, that.resultsField)
            && Objects.equals(tokenizationUpdate, that.tokenizationUpdate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxAnswerLength, numTopClasses, resultsField, tokenizationUpdate, question);
    }

    public Integer getNumTopClasses() {
        return numTopClasses;
    }

    public Integer getMaxAnswerLength() {
        return maxAnswerLength;
    }

    public String getQuestion() {
        return question;
    }

    public static class Builder
        implements
            InferenceConfigUpdate.Builder<QuestionAnsweringConfigUpdate.Builder, QuestionAnsweringConfigUpdate> {
        private Integer numTopClasses;
        private Integer maxAnswerLength;
        private String resultsField;
        private TokenizationUpdate tokenizationUpdate;
        private String question;

        @Override
        public QuestionAnsweringConfigUpdate.Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public Builder setNumTopClasses(Integer numTopClasses) {
            this.numTopClasses = numTopClasses;
            return this;
        }

        public Builder setMaxAnswerLength(Integer maxAnswerLength) {
            this.maxAnswerLength = maxAnswerLength;
            return this;
        }

        public Builder setTokenizationUpdate(TokenizationUpdate tokenizationUpdate) {
            this.tokenizationUpdate = tokenizationUpdate;
            return this;
        }

        public Builder setQuestion(String question) {
            this.question = question;
            return this;
        }

        @Override
        public QuestionAnsweringConfigUpdate build() {
            return new QuestionAnsweringConfigUpdate(question, numTopClasses, maxAnswerLength, resultsField, tokenizationUpdate);
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_3_0;
    }
}
