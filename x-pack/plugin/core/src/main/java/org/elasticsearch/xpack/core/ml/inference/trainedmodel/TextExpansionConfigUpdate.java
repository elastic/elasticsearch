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
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.TOKENIZATION;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfig.EXPANSION_TYPE;

public class TextExpansionConfigUpdate extends NlpConfigUpdate {

    public static final String NAME = TextExpansionConfig.NAME;

    public static final TextExpansionConfigUpdate EMPTY_UPDATE = new TextExpansionConfigUpdate(null, null, null);

    public static TextExpansionConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        String resultsField = (String) options.remove(RESULTS_FIELD.getPreferredName());
        String expansionTypeField = (String) options.remove(EXPANSION_TYPE.getPreferredName());
        TokenizationUpdate tokenizationUpdate = NlpConfigUpdate.tokenizationFromMap(options);

        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", options.keySet());
        }
        return new TextExpansionConfigUpdate(resultsField, expansionTypeField, tokenizationUpdate);
    }

    private static final ObjectParser<TextExpansionConfigUpdate.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<TextExpansionConfigUpdate.Builder, Void> createParser(boolean lenient) {
        ObjectParser<TextExpansionConfigUpdate.Builder, Void> parser = new ObjectParser<>(
            NAME,
            lenient,
            TextExpansionConfigUpdate.Builder::new
        );
        parser.declareString(TextExpansionConfigUpdate.Builder::setResultsField, RESULTS_FIELD);
        parser.declareString(TextExpansionConfigUpdate.Builder::setResultsField, EXPANSION_TYPE);
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
    private final String expansionType;

    public TextExpansionConfigUpdate(String resultsField, String expansionType, TokenizationUpdate tokenizationUpdate) {
        super(tokenizationUpdate);
        this.resultsField = resultsField;
        this.expansionType = expansionType;
    }

    public TextExpansionConfigUpdate(StreamInput in) throws IOException {
        super(in);
        this.resultsField = in.readOptionalString();
        this.expansionType = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(resultsField);
        out.writeOptionalString(expansionType);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        if (expansionType != null) {
            builder.field(EXPANSION_TYPE.getPreferredName(), expansionType);
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
    public boolean isSupported(InferenceConfig config) {
        return config instanceof TextExpansionConfig;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    public String getExpansionType() {
        return expansionType;
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
        return TransportVersions.V_8_7_0;
    }

    public static class Builder implements InferenceConfigUpdate.Builder<TextExpansionConfigUpdate.Builder, TextExpansionConfigUpdate> {
        private String resultsField;
        private String expansionType;
        private TokenizationUpdate tokenizationUpdate;

        @Override
        public TextExpansionConfigUpdate.Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public TextExpansionConfigUpdate.Builder setExpansionType(String expansionType) {
            this.expansionType = expansionType;
            return this;
        }

        public TextExpansionConfigUpdate.Builder setTokenizationUpdate(TokenizationUpdate tokenizationUpdate) {
            this.tokenizationUpdate = tokenizationUpdate;
            return this;
        }

        @Override
        public TextExpansionConfigUpdate build() {
            return new TextExpansionConfigUpdate(resultsField, expansionType, tokenizationUpdate);
        }
    }

}
