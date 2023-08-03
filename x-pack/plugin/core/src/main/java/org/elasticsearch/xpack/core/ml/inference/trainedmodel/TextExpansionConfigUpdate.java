/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.TOKENIZATION;

public class TextExpansionConfigUpdate extends NlpConfigUpdate {

    public static final String NAME = TextExpansionConfig.NAME;

    public static final TextExpansionConfigUpdate EMPTY_UPDATE = new TextExpansionConfigUpdate(null, null);

    public static TextExpansionConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        String resultsField = (String) options.remove(RESULTS_FIELD.getPreferredName());
        TokenizationUpdate tokenizationUpdate = NlpConfigUpdate.tokenizationFromMap(options);

        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", options.keySet());
        }
        return new TextExpansionConfigUpdate(resultsField, tokenizationUpdate);
    }

    private static final ObjectParser<TextExpansionConfigUpdate.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<TextExpansionConfigUpdate.Builder, Void> createParser(boolean lenient) {
        ObjectParser<TextExpansionConfigUpdate.Builder, Void> parser = new ObjectParser<>(
            NAME,
            lenient,
            TextExpansionConfigUpdate.Builder::new
        );
        parser.declareString(TextExpansionConfigUpdate.Builder::setResultsField, RESULTS_FIELD);
        parser.declareNamedObject(
            TextExpansionConfigUpdate.Builder::setTokenizationUpdate,
            (p, c, n) -> p.namedObject(TokenizationUpdate.class, n, lenient),
            TOKENIZATION
        );
        return parser;
    }

    public static TextExpansionConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    private final String resultsField;

    public TextExpansionConfigUpdate(String resultsField, TokenizationUpdate tokenizationUpdate) {
        super(tokenizationUpdate);
        this.resultsField = resultsField;
    }

    public TextExpansionConfigUpdate(StreamInput in) throws IOException {
        super(in);
        this.resultsField = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(resultsField);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        return builder;
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
    public InferenceConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof TextExpansionConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a request of type [{}]",
                originalConfig.getName(),
                getName()
            );
        }
        TextExpansionConfig textExpansionConfig = (TextExpansionConfig) originalConfig;
        if (isNoop(textExpansionConfig)) {
            return textExpansionConfig;
        }

        return new TextExpansionConfig(
            textExpansionConfig.getVocabularyConfig(),
            (tokenizationUpdate == null)
                ? textExpansionConfig.getTokenization()
                : tokenizationUpdate.apply(textExpansionConfig.getTokenization()),
            Optional.ofNullable(resultsField).orElse(textExpansionConfig.getResultsField())
        );
    }

    boolean isNoop(TextExpansionConfig originalConfig) {
        return (this.resultsField == null || this.resultsField.equals(originalConfig.getResultsField())) && super.isNoop();
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return config instanceof TextExpansionConfig;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new TextExpansionConfigUpdate.Builder().setResultsField(resultsField).setTokenizationUpdate(tokenizationUpdate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextExpansionConfigUpdate that = (TextExpansionConfigUpdate) o;
        return Objects.equals(resultsField, that.resultsField) && Objects.equals(tokenizationUpdate, that.tokenizationUpdate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultsField, tokenizationUpdate);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_7_0;
    }

    public static class Builder implements InferenceConfigUpdate.Builder<TextExpansionConfigUpdate.Builder, TextExpansionConfigUpdate> {
        private String resultsField;
        private TokenizationUpdate tokenizationUpdate;

        @Override
        public TextExpansionConfigUpdate.Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public TextExpansionConfigUpdate.Builder setTokenizationUpdate(TokenizationUpdate tokenizationUpdate) {
            this.tokenizationUpdate = tokenizationUpdate;
            return this;
        }

        @Override
        public TextExpansionConfigUpdate build() {
            return new TextExpansionConfigUpdate(resultsField, tokenizationUpdate);
        }
    }

}
