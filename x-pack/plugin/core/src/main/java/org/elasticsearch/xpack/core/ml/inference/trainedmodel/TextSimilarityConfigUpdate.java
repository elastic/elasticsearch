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

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.TOKENIZATION;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfig.SPAN_SCORE_COMBINATION_FUNCTION;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfig.TEXT;

public class TextSimilarityConfigUpdate extends NlpConfigUpdate implements NamedXContentObject {

    public static final String NAME = "text_similarity";

    public static TextSimilarityConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    public static TextSimilarityConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        String resultsField = (String) options.remove(RESULTS_FIELD.getPreferredName());
        String text = (String) options.remove(TEXT.getPreferredName());
        String spanScoreFunction = (String) options.remove(SPAN_SCORE_COMBINATION_FUNCTION.getPreferredName());
        TokenizationUpdate tokenizationUpdate = NlpConfigUpdate.tokenizationFromMap(options);
        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", map.keySet());
        }
        return new TextSimilarityConfigUpdate(text, resultsField, tokenizationUpdate, spanScoreFunction);
    }

    private static final ObjectParser<TextSimilarityConfigUpdate.Builder, Void> STRICT_PARSER = new ObjectParser<>(
        NAME,
        TextSimilarityConfigUpdate.Builder::new
    );

    static {
        STRICT_PARSER.declareString(Builder::setText, TEXT);
        STRICT_PARSER.declareString(Builder::setResultsField, RESULTS_FIELD);
        STRICT_PARSER.declareString(Builder::setSpanScoreFunction, SPAN_SCORE_COMBINATION_FUNCTION);
        STRICT_PARSER.declareNamedObject(
            Builder::setTokenizationUpdate,
            (p, c, n) -> p.namedObject(TokenizationUpdate.class, n, false),
            TOKENIZATION
        );
    }

    private final String text;
    private final String resultsField;
    private final TextSimilarityConfig.SpanScoreFunction spanScoreFunction;

    public TextSimilarityConfigUpdate(
        String text,
        @Nullable String resultsField,
        @Nullable TokenizationUpdate tokenizationUpdate,
        @Nullable String spanScoreFunction
    ) {
        super(tokenizationUpdate);
        this.text = ExceptionsHelper.requireNonNull(text, TEXT);
        this.resultsField = resultsField;
        this.spanScoreFunction = Optional.ofNullable(spanScoreFunction)
            .map(TextSimilarityConfig.SpanScoreFunction::fromString)
            .orElse(null);
    }

    public TextSimilarityConfigUpdate(StreamInput in) throws IOException {
        super(in);
        text = in.readString();
        resultsField = in.readOptionalString();
        spanScoreFunction = in.readOptionalEnum(TextSimilarityConfig.SpanScoreFunction.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(text);
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
        builder.field(TEXT.getPreferredName(), text);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public InferenceConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof TextSimilarityConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a inference request of type [{}]",
                originalConfig.getName(),
                getName()
            );
        }

        TextSimilarityConfig textSimilarityConfig = (TextSimilarityConfig) originalConfig;
        return new TextSimilarityConfig(
            text,
            textSimilarityConfig.getVocabularyConfig(),
            tokenizationUpdate == null
                ? textSimilarityConfig.getTokenization()
                : tokenizationUpdate.apply(textSimilarityConfig.getTokenization()),
            Optional.ofNullable(resultsField).orElse(textSimilarityConfig.getResultsField()),
            Optional.ofNullable(spanScoreFunction).orElse(textSimilarityConfig.getSpanScoreFunction())
        );
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return config instanceof TextSimilarityConfig;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder().setText(text).setResultsField(resultsField).setTokenizationUpdate(tokenizationUpdate);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TextSimilarityConfigUpdate that = (TextSimilarityConfigUpdate) o;
        return Objects.equals(text, that.text)
            && Objects.equals(resultsField, that.resultsField)
            && Objects.equals(tokenizationUpdate, that.tokenizationUpdate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultsField, tokenizationUpdate, text);
    }

    public String getText() {
        return text;
    }

    public static class Builder implements InferenceConfigUpdate.Builder<TextSimilarityConfigUpdate.Builder, TextSimilarityConfigUpdate> {
        private String resultsField;
        private String spanScoreFunction;
        private TokenizationUpdate tokenizationUpdate;
        private String text;

        @Override
        public TextSimilarityConfigUpdate.Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public Builder setTokenizationUpdate(TokenizationUpdate tokenizationUpdate) {
            this.tokenizationUpdate = tokenizationUpdate;
            return this;
        }

        public Builder setText(String text) {
            this.text = text;
            return this;
        }

        public Builder setSpanScoreFunction(String spanScoreFunction) {
            this.spanScoreFunction = spanScoreFunction;
            return this;
        }

        @Override
        public TextSimilarityConfigUpdate build() {
            return new TextSimilarityConfigUpdate(text, resultsField, tokenizationUpdate, spanScoreFunction);
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_5_0;
    }
}
