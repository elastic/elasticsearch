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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.MapParsingUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.extractRequiredString;

/**
 * Defines the task settings for the openai service.
 *
 * @param model the id of the model to use in the requests to openai
 * @param user an optional unique identifier representing the end-user, which can help OpenAI to monitor and detect abuse
 *             <a href="https://platform.openai.com/docs/api-reference/embeddings/create">see the openai docs for more details</a>
 */
public record OpenAiEmbeddingsTaskSettings(String model, @Nullable String user) implements TaskSettings {

    public static final String NAME = "openai_embeddings_task_settings";
    public static final String MODEL = "model";
    public static final String USER = "user";

    public static OpenAiEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String model = extractRequiredString(map, MODEL, ModelConfigurations.TASK_SETTINGS, validationException);
        String user = extractOptionalString(map, USER, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiEmbeddingsTaskSettings(model, user);
    }

    public OpenAiEmbeddingsTaskSettings {
        Objects.requireNonNull(model);
    }

    public OpenAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalString());
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

    public OpenAiEmbeddingsTaskSettings overrideWith(OpenAiEmbeddingsRequestTaskSettings requestSettings) {
        var modelToUse = requestSettings.model() == null ? model : requestSettings.model();
        var userToUse = requestSettings.user() == null ? user : requestSettings.user();

        return new OpenAiEmbeddingsTaskSettings(modelToUse, userToUse);
    }
}
