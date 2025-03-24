/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.TOKENIZATION;

public class TextEmbeddingConfigUpdate extends NlpConfigUpdate implements NamedXContentObject {

    public static final String NAME = TextEmbeddingConfig.NAME;

    public static final TextEmbeddingConfigUpdate EMPTY_INSTANCE = new TextEmbeddingConfigUpdate(null, null);

    public static TextEmbeddingConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        String resultsField = (String) options.remove(RESULTS_FIELD.getPreferredName());
        TokenizationUpdate tokenizationUpdate = NlpConfigUpdate.tokenizationFromMap(options);

        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", options.keySet());
        }
        return new TextEmbeddingConfigUpdate(resultsField, tokenizationUpdate);
    }

    private static final ObjectParser<TextEmbeddingConfigUpdate.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<TextEmbeddingConfigUpdate.Builder, Void> createParser(boolean lenient) {
        ObjectParser<TextEmbeddingConfigUpdate.Builder, Void> parser = new ObjectParser<>(
            NAME,
            lenient,
            TextEmbeddingConfigUpdate.Builder::new
        );
        parser.declareString(TextEmbeddingConfigUpdate.Builder::setResultsField, RESULTS_FIELD);
        parser.declareNamedObject(
            TextEmbeddingConfigUpdate.Builder::setTokenizationUpdate,
            (p, c, n) -> p.namedObject(TokenizationUpdate.class, n, lenient),
            TOKENIZATION
        );
        return parser;
    }

    public static TextEmbeddingConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    private final String resultsField;

    public TextEmbeddingConfigUpdate(String resultsField, TokenizationUpdate tokenizationUpdate) {
        super(tokenizationUpdate);
        this.resultsField = resultsField;
    }

    public TextEmbeddingConfigUpdate(StreamInput in) throws IOException {
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
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_0_0;
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return config instanceof TextEmbeddingConfig;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder().setResultsField(resultsField).setTokenizationUpdate(tokenizationUpdate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextEmbeddingConfigUpdate that = (TextEmbeddingConfigUpdate) o;
        return Objects.equals(resultsField, that.resultsField) && Objects.equals(tokenizationUpdate, that.tokenizationUpdate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultsField, tokenizationUpdate);
    }

    public static class Builder implements InferenceConfigUpdate.Builder<TextEmbeddingConfigUpdate.Builder, TextEmbeddingConfigUpdate> {
        private String resultsField;
        private TokenizationUpdate tokenizationUpdate;

        @Override
        public Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public TextEmbeddingConfigUpdate.Builder setTokenizationUpdate(TokenizationUpdate tokenizationUpdate) {
            this.tokenizationUpdate = tokenizationUpdate;
            return this;
        }

        @Override
        public TextEmbeddingConfigUpdate build() {
            return new TextEmbeddingConfigUpdate(resultsField, tokenizationUpdate);
        }
    }
}
