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

public class NerConfigUpdate extends NlpConfigUpdate {
    public static final String NAME = NerConfig.NAME;

    public static NerConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        String resultsField = (String) options.remove(RESULTS_FIELD.getPreferredName());
        TokenizationUpdate tokenizationUpdate = NlpConfigUpdate.tokenizationFromMap(options);

        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", options.keySet());
        }
        return new NerConfigUpdate(resultsField, tokenizationUpdate);
    }

    private static final ObjectParser<NerConfigUpdate.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<NerConfigUpdate.Builder, Void> createParser(boolean lenient) {
        ObjectParser<NerConfigUpdate.Builder, Void> parser = new ObjectParser<>(NAME, lenient, NerConfigUpdate.Builder::new);
        parser.declareString(NerConfigUpdate.Builder::setResultsField, RESULTS_FIELD);
        parser.declareNamedObject(
            NerConfigUpdate.Builder::setTokenizationUpdate,
            (p, c, n) -> p.namedObject(TokenizationUpdate.class, n, lenient),
            TOKENIZATION
        );
        return parser;
    }

    public static NerConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    private final String resultsField;

    public NerConfigUpdate(String resultsField, TokenizationUpdate tokenizationUpdate) {
        super(tokenizationUpdate);
        this.resultsField = resultsField;
    }

    public NerConfigUpdate(StreamInput in) throws IOException {
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
        if (originalConfig instanceof NerConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a request of type [{}]",
                originalConfig.getName(),
                getName()
            );
        }
        NerConfig nerConfig = (NerConfig) originalConfig;
        if (isNoop(nerConfig)) {
            return nerConfig;
        }

        return new NerConfig(
            nerConfig.getVocabularyConfig(),
            (tokenizationUpdate == null) ? nerConfig.getTokenization() : tokenizationUpdate.apply(nerConfig.getTokenization()),
            nerConfig.getClassificationLabels(),
            Optional.ofNullable(resultsField).orElse(nerConfig.getResultsField())
        );
    }

    boolean isNoop(NerConfig originalConfig) {
        return (this.resultsField == null || this.resultsField.equals(originalConfig.getResultsField())) && super.isNoop();
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return config instanceof NerConfig;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new NerConfigUpdate.Builder().setResultsField(resultsField).setTokenizationUpdate(tokenizationUpdate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NerConfigUpdate that = (NerConfigUpdate) o;
        return Objects.equals(resultsField, that.resultsField) && Objects.equals(tokenizationUpdate, that.tokenizationUpdate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultsField, tokenizationUpdate);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_0_0;
    }

    public static class Builder implements InferenceConfigUpdate.Builder<NerConfigUpdate.Builder, NerConfigUpdate> {
        private String resultsField;
        private TokenizationUpdate tokenizationUpdate;

        @Override
        public NerConfigUpdate.Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public NerConfigUpdate.Builder setTokenizationUpdate(TokenizationUpdate tokenizationUpdate) {
            this.tokenizationUpdate = tokenizationUpdate;
            return this;
        }

        @Override
        public NerConfigUpdate build() {
            return new NerConfigUpdate(resultsField, tokenizationUpdate);
        }
    }
}
