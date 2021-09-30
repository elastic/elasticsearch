/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.RESULTS_FIELD;

public class NerConfigUpdate extends NlpConfigUpdate implements NamedXContentObject {
    public static final String NAME = NerConfig.NAME;

    public static NerConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        String resultsField = (String)options.remove(RESULTS_FIELD.getPreferredName());

        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", options.keySet());
        }
        return new NerConfigUpdate(resultsField);
    }

    private static final ObjectParser<NerConfigUpdate.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<NerConfigUpdate.Builder, Void> createParser(boolean lenient) {
        ObjectParser<NerConfigUpdate.Builder, Void> parser = new ObjectParser<>(
            NAME,
            lenient,
            NerConfigUpdate.Builder::new);
        parser.declareString(NerConfigUpdate.Builder::setResultsField, RESULTS_FIELD);
        return parser;
    }

    public static NerConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    private final String resultsField;

    public NerConfigUpdate(String resultsField) {
        this.resultsField = resultsField;
    }

    public NerConfigUpdate(StreamInput in) throws IOException {
        this.resultsField = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(resultsField);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        builder.endObject();
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
        if (resultsField == null || resultsField.equals(originalConfig.getResultsField())) {
            return originalConfig;
        }

        if (originalConfig instanceof NerConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a inference request of type [{}]",
                originalConfig.getName(),
                getName());
        }

        NerConfig nerConfig = (NerConfig)originalConfig;
        return new NerConfig(
            nerConfig.getVocabularyConfig(),
            nerConfig.getTokenization(),
            nerConfig.getClassificationLabels(),
            resultsField);
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
        return new NerConfigUpdate.Builder()
            .setResultsField(resultsField);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NerConfigUpdate that = (NerConfigUpdate) o;
        return Objects.equals(resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultsField);
    }

    public static class Builder
        implements InferenceConfigUpdate.Builder<NerConfigUpdate.Builder, NerConfigUpdate> {
        private String resultsField;

        @Override
        public NerConfigUpdate.Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public NerConfigUpdate build() {
            return new NerConfigUpdate(this.resultsField);
        }
    }
}

