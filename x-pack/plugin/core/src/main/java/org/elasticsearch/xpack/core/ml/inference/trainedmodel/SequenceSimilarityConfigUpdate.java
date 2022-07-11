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
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.TOKENIZATION;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.SequenceSimilarityConfig.SEQUENCE;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.SequenceSimilarityConfig.SPAN_SCORE_COMBINATION_FUNCTION;

public class SequenceSimilarityConfigUpdate extends NlpConfigUpdate implements NamedXContentObject {

    public static final String NAME = "sequence_similarity";

    public static SequenceSimilarityConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    public static SequenceSimilarityConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        String resultsField = (String) options.remove(RESULTS_FIELD.getPreferredName());
        String question = (String) options.remove(SEQUENCE.getPreferredName());
        String spanScoreFunction = (String) options.remove(SPAN_SCORE_COMBINATION_FUNCTION.getPreferredName());
        TokenizationUpdate tokenizationUpdate = NlpConfigUpdate.tokenizationFromMap(options);
        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", map.keySet());
        }
        return new SequenceSimilarityConfigUpdate(question, resultsField, tokenizationUpdate, spanScoreFunction);
    }

    private static final ObjectParser<SequenceSimilarityConfigUpdate.Builder, Void> STRICT_PARSER = new ObjectParser<>(
        NAME,
        SequenceSimilarityConfigUpdate.Builder::new
    );

    static {
        STRICT_PARSER.declareString(Builder::setSequence, SEQUENCE);
        STRICT_PARSER.declareString(Builder::setResultsField, RESULTS_FIELD);
        STRICT_PARSER.declareString(Builder::setSpanScoreFunction, SPAN_SCORE_COMBINATION_FUNCTION);
        STRICT_PARSER.declareNamedObject(
            Builder::setTokenizationUpdate,
            (p, c, n) -> p.namedObject(TokenizationUpdate.class, n, false),
            TOKENIZATION
        );
    }

    private final String sequence;
    private final String resultsField;
    private final SequenceSimilarityConfig.SpanScoreFunction spanScoreFunction;

    public SequenceSimilarityConfigUpdate(
        String question,
        @Nullable String resultsField,
        @Nullable TokenizationUpdate tokenizationUpdate,
        @Nullable String spanScoreFunction
    ) {
        super(tokenizationUpdate);
        this.sequence = ExceptionsHelper.requireNonNull(question, SEQUENCE);
        this.resultsField = resultsField;
        this.spanScoreFunction = Optional.ofNullable(spanScoreFunction)
            .map(SequenceSimilarityConfig.SpanScoreFunction::fromString)
            .orElse(null);
    }

    public SequenceSimilarityConfigUpdate(StreamInput in) throws IOException {
        super(in);
        sequence = in.readString();
        resultsField = in.readOptionalString();
        spanScoreFunction = in.readOptionalEnum(SequenceSimilarityConfig.SpanScoreFunction.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sequence);
        out.writeOptionalString(resultsField);
        out.writeOptionalEnum(spanScoreFunction);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        if (spanScoreFunction != null) {
            builder.field(SPAN_SCORE_COMBINATION_FUNCTION.getPreferredName(), spanScoreFunction);
        }
        builder.field(SEQUENCE.getPreferredName(), sequence);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public InferenceConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof SequenceSimilarityConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a inference request of type [{}]",
                originalConfig.getName(),
                getName()
            );
        }

        SequenceSimilarityConfig sequenceSimilarityConfig = (SequenceSimilarityConfig) originalConfig;
        return new SequenceSimilarityConfig(
            sequence,
            sequenceSimilarityConfig.getVocabularyConfig(),
            tokenizationUpdate == null
                ? sequenceSimilarityConfig.getTokenization()
                : tokenizationUpdate.apply(sequenceSimilarityConfig.getTokenization()),
            Optional.ofNullable(resultsField).orElse(sequenceSimilarityConfig.getResultsField()),
            Optional.ofNullable(spanScoreFunction).orElse(sequenceSimilarityConfig.getSpanScoreFunction())
        );
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return config instanceof SequenceSimilarityConfig;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder().setSequence(sequence).setResultsField(resultsField).setTokenizationUpdate(tokenizationUpdate);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SequenceSimilarityConfigUpdate that = (SequenceSimilarityConfigUpdate) o;
        return Objects.equals(sequence, that.sequence)
            && Objects.equals(resultsField, that.resultsField)
            && Objects.equals(tokenizationUpdate, that.tokenizationUpdate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultsField, tokenizationUpdate, sequence);
    }

    public String getSequence() {
        return sequence;
    }

    public static class Builder
        implements
            InferenceConfigUpdate.Builder<SequenceSimilarityConfigUpdate.Builder, SequenceSimilarityConfigUpdate> {
        private String resultsField;
        private String spanScoreFunction;
        private TokenizationUpdate tokenizationUpdate;
        private String sequence;

        @Override
        public SequenceSimilarityConfigUpdate.Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public Builder setTokenizationUpdate(TokenizationUpdate tokenizationUpdate) {
            this.tokenizationUpdate = tokenizationUpdate;
            return this;
        }

        public Builder setSequence(String sequence) {
            this.sequence = sequence;
            return this;
        }

        public Builder setSpanScoreFunction(String spanScoreFunction) {
            this.spanScoreFunction = spanScoreFunction;
            return this;
        }

        @Override
        public SequenceSimilarityConfigUpdate build() {
            return new SequenceSimilarityConfigUpdate(sequence, resultsField, tokenizationUpdate, spanScoreFunction);
        }
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_4_0;
    }
}
