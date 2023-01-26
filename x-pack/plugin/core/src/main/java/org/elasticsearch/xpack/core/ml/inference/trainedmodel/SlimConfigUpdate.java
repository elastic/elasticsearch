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

public class SlimConfigUpdate extends NlpConfigUpdate {

    public static final String NAME = SlimConfig.NAME;

    public static SlimConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        String resultsField = (String) options.remove(RESULTS_FIELD.getPreferredName());
        TokenizationUpdate tokenizationUpdate = NlpConfigUpdate.tokenizationFromMap(options);

        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", options.keySet());
        }
        return new SlimConfigUpdate(resultsField, tokenizationUpdate);
    }

    private static final ObjectParser<SlimConfigUpdate.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<SlimConfigUpdate.Builder, Void> createParser(boolean lenient) {
        ObjectParser<SlimConfigUpdate.Builder, Void> parser = new ObjectParser<>(NAME, lenient, SlimConfigUpdate.Builder::new);
        parser.declareString(SlimConfigUpdate.Builder::setResultsField, RESULTS_FIELD);
        parser.declareNamedObject(
            SlimConfigUpdate.Builder::setTokenizationUpdate,
            (p, c, n) -> p.namedObject(TokenizationUpdate.class, n, lenient),
            TOKENIZATION
        );
        return parser;
    }

    public static SlimConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    private final String resultsField;

    public SlimConfigUpdate(String resultsField, TokenizationUpdate tokenizationUpdate) {
        super(tokenizationUpdate);
        this.resultsField = resultsField;
    }

    public SlimConfigUpdate(StreamInput in) throws IOException {
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
        if (originalConfig instanceof SlimConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a request of type [{}]",
                originalConfig.getName(),
                getName()
            );
        }
        SlimConfig SlimConfig = (SlimConfig) originalConfig;
        if (isNoop(SlimConfig)) {
            return SlimConfig;
        }

        return new SlimConfig(
            SlimConfig.getVocabularyConfig(),
            (tokenizationUpdate == null) ? SlimConfig.getTokenization() : tokenizationUpdate.apply(SlimConfig.getTokenization()),
            Optional.ofNullable(resultsField).orElse(SlimConfig.getResultsField())
        );
    }

    boolean isNoop(SlimConfig originalConfig) {
        return (this.resultsField == null || this.resultsField.equals(originalConfig.getResultsField())) && super.isNoop();
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return config instanceof SlimConfig;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new SlimConfigUpdate.Builder().setResultsField(resultsField).setTokenizationUpdate(tokenizationUpdate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SlimConfigUpdate that = (SlimConfigUpdate) o;
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

    public static class Builder implements InferenceConfigUpdate.Builder<SlimConfigUpdate.Builder, SlimConfigUpdate> {
        private String resultsField;
        private TokenizationUpdate tokenizationUpdate;

        @Override
        public SlimConfigUpdate.Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public SlimConfigUpdate.Builder setTokenizationUpdate(TokenizationUpdate tokenizationUpdate) {
            this.tokenizationUpdate = tokenizationUpdate;
            return this;
        }

        @Override
        public SlimConfigUpdate build() {
            return new SlimConfigUpdate(resultsField, tokenizationUpdate);
        }
    }

}
