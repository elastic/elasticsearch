/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.pytorch;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;

import java.io.IOException;
import java.util.Objects;

public class PyTorchModel implements LenientlyParsedTrainedModel, StrictlyParsedTrainedModel {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(PyTorchModel.class);

    public static final ParseField NAME = new ParseField("pytorch");
    public static final ParseField MODEL_ID = new ParseField("model_id");

    private static final ObjectParser<PyTorchModel.Builder, Void> LENIENT_PARSER = createParser(true);
    private static final ObjectParser<PyTorchModel.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<PyTorchModel.Builder, Void> createParser(boolean lenient) {
        ObjectParser<PyTorchModel.Builder, Void> parser = new ObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            PyTorchModel.Builder::new);
        parser.declareString(PyTorchModel.Builder::setModelId, MODEL_ID);
        parser.declareString(PyTorchModel.Builder::setTargetType, TargetType.TARGET_TYPE);
        return parser;
    }

    public static PyTorchModel fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? fromXContentLenient(parser) : fromXContentStrict(parser);
    }

    public static PyTorchModel fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    public static PyTorchModel fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null).build();
    }

    private final String modelId;
    private final TargetType targetType;

    public PyTorchModel(String modelId, TargetType targetType) {
        this.modelId = Objects.requireNonNull(modelId);
        this.targetType = Objects.requireNonNull(targetType);
    }

    public PyTorchModel(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.targetType = in.readEnum(TargetType.class);
    }

    @Override
    public TargetType targetType() {
        return targetType;
    }

    public String getModelId() {
        return modelId;
    }

    @Override
    public void validate() {

    }

    @Override
    public long estimatedNumOperations() {
        return 0;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE;
    }

    public Version getMinimalCompatibilityVersion() {
        return Version.V_8_0_0; // TODO adjust on backport
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        targetType.writeTo(out);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID.getPreferredName(), modelId);
        builder.field(TargetType.TARGET_TYPE.getPreferredName(), targetType);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PyTorchModel that = (PyTorchModel) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(targetType, that.targetType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, targetType);
    }

    private static class Builder {

        private String modelId;
        private TargetType targetType;

        public Builder setModelId(String modelId) {
            this.modelId = modelId;
            return this;
        }

        public Builder setTargetType(TargetType targetType) {
            this.targetType = targetType;
            return this;
        }

        public Builder setTargetType(String targetType) {
            this.targetType = TargetType.fromString(targetType);
            return this;
        }

        PyTorchModel build() {
            return new PyTorchModel(modelId, targetType);
        }
    }
}
