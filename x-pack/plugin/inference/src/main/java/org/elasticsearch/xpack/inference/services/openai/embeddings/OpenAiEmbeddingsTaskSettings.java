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
    public static final String USER = "user";

    private final String model;
    private final String user;

    public static OpenAiEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String model = MapParsingUtils.removeAsType(map, MODEL, String.class);

        if (model == null) {
            validationException.addValidationError(MapParsingUtils.missingSettingErrorMsg(MODEL, Model.TASK_SETTINGS));
        } else if (model.isEmpty()) {
            validationException.addValidationError(MapParsingUtils.mustBeNonEmptyString(MODEL));
        }

        String user = MapParsingUtils.removeAsType(map, USER, String.class);

        if (user != null && user.isEmpty()) {
            validationException.addValidationError(MapParsingUtils.mustBeNonEmptyString(USER));
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiEmbeddingsTaskSettings(model, user);
    }

    public OpenAiEmbeddingsTaskSettings(String model, String user) {
        this.model = Objects.requireNonNull(model);
        // user is optional in the openai embeddings spec: https://platform.openai.com/docs/api-reference/embeddings/create
        this.user = user;
    }

    public OpenAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        model = in.readString();
        user = in.readOptionalString();
    }

    public String getModel() {
        return model;
    }

    public String getUser() {
        return user;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL, model);
        // TODO is it ok to have null here?
        builder.field(USER, user);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // TODO change this
        return TransportVersions.V_8_500_072;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(model);
        out.writeOptionalString(user);
    }

    @Override
    public int hashCode() {
        return Objects.hash(model, user);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenAiEmbeddingsTaskSettings that = (OpenAiEmbeddingsTaskSettings) o;

        return model.equals(that.model) && Objects.equals(user, that.user);
    }
}
