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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.TOKENIZATION;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig.LABELS;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig.MULTI_LABEL;

public class ZeroShotClassificationConfigUpdate extends NlpConfigUpdate implements NamedXContentObject {

    public static final String NAME = "zero_shot_classification";

    public static ZeroShotClassificationConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    @SuppressWarnings({ "unchecked" })
    public static ZeroShotClassificationConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        Boolean isMultiLabel = (Boolean) options.remove(MULTI_LABEL.getPreferredName());
        List<String> labels = (List<String>) options.remove(LABELS.getPreferredName());
        String resultsField = (String) options.remove(RESULTS_FIELD.getPreferredName());
        TokenizationUpdate tokenizationUpdate = NlpConfigUpdate.tokenizationFromMap(options);
        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", map.keySet());
        }
        return new ZeroShotClassificationConfigUpdate(labels, isMultiLabel, resultsField, tokenizationUpdate);
    }

    @SuppressWarnings({ "unchecked" })
    private static final ObjectParser<ZeroShotClassificationConfigUpdate.Builder, Void> STRICT_PARSER = new ObjectParser<>(
        NAME,
        ZeroShotClassificationConfigUpdate.Builder::new
    );

    static {
        STRICT_PARSER.declareStringArray(Builder::setLabels, LABELS);
        STRICT_PARSER.declareBoolean(Builder::setMultiLabel, MULTI_LABEL);
        STRICT_PARSER.declareString(Builder::setResultsField, RESULTS_FIELD);
        STRICT_PARSER.declareNamedObject(
            Builder::setTokenizationUpdate,
            (p, c, n) -> p.namedObject(TokenizationUpdate.class, n, false),
            TOKENIZATION
        );
    }

    private final List<String> labels;
    private final Boolean isMultiLabel;
    private final String resultsField;

    public ZeroShotClassificationConfigUpdate(
        @Nullable List<String> labels,
        @Nullable Boolean isMultiLabel,
        @Nullable String resultsField,
        @Nullable TokenizationUpdate tokenizationUpdate
    ) {
        super(tokenizationUpdate);
        this.labels = labels;
        if (labels != null && labels.isEmpty()) {
            throw ExceptionsHelper.badRequestException("[{}] must not be empty", LABELS.getPreferredName());
        }
        this.isMultiLabel = isMultiLabel;
        this.resultsField = resultsField;
    }

    public ZeroShotClassificationConfigUpdate(StreamInput in) throws IOException {
        super(in);
        labels = in.readOptionalStringList();
        isMultiLabel = in.readOptionalBoolean();
        resultsField = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalStringCollection(labels);
        out.writeOptionalBoolean(isMultiLabel);
        out.writeOptionalString(resultsField);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (labels != null) {
            builder.field(LABELS.getPreferredName(), labels);
        }
        if (isMultiLabel != null) {
            builder.field(MULTI_LABEL.getPreferredName(), isMultiLabel);
        }
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
    public InferenceConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof ZeroShotClassificationConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a inference request of type [{}]",
                originalConfig.getName(),
                getName()
            );
        }

        ZeroShotClassificationConfig zeroShotConfig = (ZeroShotClassificationConfig) originalConfig;
        if ((labels == null || labels.isEmpty()) && (zeroShotConfig.getLabels() == null || zeroShotConfig.getLabels().isEmpty())) {
            throw ExceptionsHelper.badRequestException(
                "stored configuration has no [{}] defined, supplied inference_config update must supply [{}]",
                LABELS.getPreferredName(),
                LABELS.getPreferredName()
            );
        }
        if (isNoop(zeroShotConfig)) {
            return originalConfig;
        }
        return new ZeroShotClassificationConfig(
            zeroShotConfig.getClassificationLabels(),
            zeroShotConfig.getVocabularyConfig(),
            tokenizationUpdate == null ? zeroShotConfig.getTokenization() : tokenizationUpdate.apply(zeroShotConfig.getTokenization()),
            zeroShotConfig.getHypothesisTemplate(),
            Optional.ofNullable(isMultiLabel).orElse(zeroShotConfig.isMultiLabel()),
            Optional.ofNullable(labels).orElse(zeroShotConfig.getLabels().orElse(null)),
            Optional.ofNullable(resultsField).orElse(zeroShotConfig.getResultsField())
        );
    }

    boolean isNoop(ZeroShotClassificationConfig originalConfig) {
        return (labels == null || labels.equals(originalConfig.getLabels().orElse(null)))
            && (isMultiLabel == null || isMultiLabel.equals(originalConfig.isMultiLabel()))
            && (resultsField == null || resultsField.equals(originalConfig.getResultsField()))
            && super.isNoop();
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return config instanceof ZeroShotClassificationConfig;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder().setLabels(labels)
            .setMultiLabel(isMultiLabel)
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

        ZeroShotClassificationConfigUpdate that = (ZeroShotClassificationConfigUpdate) o;
        return Objects.equals(isMultiLabel, that.isMultiLabel)
            && Objects.equals(labels, that.labels)
            && Objects.equals(resultsField, that.resultsField)
            && Objects.equals(tokenizationUpdate, that.tokenizationUpdate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(labels, isMultiLabel, resultsField, tokenizationUpdate);
    }

    public List<String> getLabels() {
        return labels;
    }

    public Boolean getMultiLabel() {
        return isMultiLabel;
    }

    public static class Builder
        implements
            InferenceConfigUpdate.Builder<ZeroShotClassificationConfigUpdate.Builder, ZeroShotClassificationConfigUpdate> {
        private List<String> labels;
        private Boolean isMultiLabel;
        private String resultsField;
        private TokenizationUpdate tokenizationUpdate;

        @Override
        public ZeroShotClassificationConfigUpdate.Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public Builder setLabels(List<String> labels) {
            this.labels = labels;
            return this;
        }

        public Builder setMultiLabel(Boolean multiLabel) {
            isMultiLabel = multiLabel;
            return this;
        }

        public Builder setTokenizationUpdate(TokenizationUpdate tokenizationUpdate) {
            this.tokenizationUpdate = tokenizationUpdate;
            return this;
        }

        @Override
        public ZeroShotClassificationConfigUpdate build() {
            return new ZeroShotClassificationConfigUpdate(labels, isMultiLabel, resultsField, tokenizationUpdate);
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_0_0;
    }
}
