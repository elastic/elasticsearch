/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig.LABELS;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig.MULTI_LABEL;

public class ZeroShotClassificationConfigUpdate extends NlpConfigUpdate implements NamedXContentObject {

    public static final String NAME = "zero_shot_classification";

    public static ZeroShotClassificationConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    @SuppressWarnings({ "unchecked"})
    public static ZeroShotClassificationConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        Boolean isMultiLabel = (Boolean)options.remove(MULTI_LABEL.getPreferredName());
        List<String> labels = (List<String>)options.remove(LABELS.getPreferredName());
        String resultsField = (String)options.remove(RESULTS_FIELD.getPreferredName());
        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", map.keySet());
        }
        return new ZeroShotClassificationConfigUpdate(labels, isMultiLabel, resultsField);
    }

    @SuppressWarnings({ "unchecked"})
    private static final ConstructingObjectParser<ZeroShotClassificationConfigUpdate, Void> STRICT_PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new ZeroShotClassificationConfigUpdate((List<String>)a[0], (Boolean) a[1], (String) a[2])
    );

    static {
        STRICT_PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), LABELS);
        STRICT_PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), MULTI_LABEL);
        STRICT_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), RESULTS_FIELD);
    }

    private final List<String> labels;
    private final Boolean isMultiLabel;
    private final String resultsField;

    public ZeroShotClassificationConfigUpdate(
        @Nullable List<String> labels,
        @Nullable Boolean isMultiLabel,
        @Nullable String resultsField
    ) {
        this.labels = labels;
        if (labels != null && labels.isEmpty()) {
            throw ExceptionsHelper.badRequestException("[{}] must not be empty", LABELS.getPreferredName());
        }
        this.isMultiLabel = isMultiLabel;
        this.resultsField = resultsField;
    }

    public ZeroShotClassificationConfigUpdate(StreamInput in) throws IOException {
        labels = in.readOptionalStringList();
        isMultiLabel = in.readOptionalBoolean();
        resultsField = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStringCollection(labels);
        out.writeOptionalBoolean(isMultiLabel);
        out.writeOptionalString(resultsField);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (labels != null) {
            builder.field(LABELS.getPreferredName(), labels);
        }
        if (isMultiLabel != null) {
            builder.field(MULTI_LABEL.getPreferredName(), isMultiLabel);
        }
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
    public InferenceConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof ZeroShotClassificationConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a inference request of type [{}]",
                originalConfig.getName(),
                getName());
        }

        ZeroShotClassificationConfig zeroShotConfig = (ZeroShotClassificationConfig)originalConfig;
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
            zeroShotConfig.getTokenization(),
            zeroShotConfig.getHypothesisTemplate(),
            Optional.ofNullable(isMultiLabel).orElse(zeroShotConfig.isMultiLabel()),
            Optional.ofNullable(labels).orElse(zeroShotConfig.getLabels()),
            Optional.ofNullable(resultsField).orElse(zeroShotConfig.getResultsField())
        );
    }

    boolean isNoop(ZeroShotClassificationConfig originalConfig) {
        return (labels == null || labels.equals(originalConfig.getClassificationLabels()))
            && (isMultiLabel == null || isMultiLabel.equals(originalConfig.isMultiLabel()))
            && (resultsField == null || resultsField.equals(originalConfig.getResultsField()));
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
        return new Builder().setLabels(labels).setMultiLabel(isMultiLabel);
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
        return Objects.equals(isMultiLabel, that.isMultiLabel) &&
            Objects.equals(labels, that.labels) &&
            Objects.equals(resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(labels, isMultiLabel, resultsField);
    }

    public List<String> getLabels() {
        return labels;
    }

    public static class Builder implements InferenceConfigUpdate.Builder<
        ZeroShotClassificationConfigUpdate.Builder,
        ZeroShotClassificationConfigUpdate
        > {
        private List<String> labels;
        private Boolean isMultiLabel;
        private String resultsField;

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

        public ZeroShotClassificationConfigUpdate build() {
            return new ZeroShotClassificationConfigUpdate(labels, isMultiLabel, resultsField);
        }
    }
}
