/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.TaskSettings;
import org.elasticsearch.xpack.inference.services.MapParsingUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class OpenAiEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "openai_task_settings";
    public static final String MODEL = "model";

    private final String model;

    public static OpenAiEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String model = MapParsingUtils.removeAsType(map, MODEL, String.class);

        if (model == null) {
            validationException.addValidationError(MapParsingUtils.missingSettingErrorMsg(MODEL, Model.SERVICE_SETTINGS));
        } else if (model.isEmpty()) {
            validationException.addValidationError(MapParsingUtils.mustBeNonEmptyString(MODEL));
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiEmbeddingsTaskSettings(model);
    }

    public OpenAiEmbeddingsTaskSettings(String model) {
        this.model = model;
    }

    public OpenAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        model = in.readString();
    }

    public String getModel() {
        return model;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL, model);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_500_072;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(model);
    }

    @Override
    public int hashCode() {
        return Objects.hash(model);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenAiEmbeddingsTaskSettings that = (OpenAiEmbeddingsTaskSettings) o;
        return model.equals(that.model);
    }

}
