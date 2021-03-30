/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.inference.trainedmodel.pytorch;

import org.elasticsearch.client.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.client.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class PyTorchModel implements TrainedModel {

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

    public String getModelId() {
        return modelId;
    }

    public TargetType getTargetType() {
        return targetType;
    }

    @Override
    public List<String> getFeatureNames() {
        return Collections.emptyList();
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

    public static class Builder {

        private String modelId;
        private TargetType targetType;

        public Builder setModelId(String modelId) {
            this.modelId = modelId;
            return this;
        }

        public void setTargetType(TargetType targetType) {
            this.targetType = targetType;
        }

        public void setTargetType(String targetType) {
            this.targetType = TargetType.fromString(targetType);
        }

        PyTorchModel build() {
            return new PyTorchModel(modelId, targetType);
        }
    }
}
