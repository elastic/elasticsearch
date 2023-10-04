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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.MapParsingUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public record OpenAiEmbeddingsTaskSettings(String model, String user) implements TaskSettings {

    public static final String NAME = "openai_task_settings";
    public static final String MODEL = "model";
    public static final String USER = "user";

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

    public OpenAiEmbeddingsTaskSettings {
        Objects.requireNonNull(model);
        // user is optional in the openai embeddings spec: https://platform.openai.com/docs/api-reference/embeddings/create
    }

    public OpenAiEmbeddingsTaskSettings overrideWith(OpenAiEmbeddingsRequestTaskSettings requestSettings) {
        var modelToUse = requestSettings.model() == null ? model : requestSettings.model();
        var userToUse = requestSettings.user() == null ? user : requestSettings.user();

        return new OpenAiEmbeddingsTaskSettings(modelToUse, userToUse);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL, model);
        if (user != null) {
            builder.field(USER, user);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_OPENAI_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(model);
        out.writeOptionalString(user);
    }
}
