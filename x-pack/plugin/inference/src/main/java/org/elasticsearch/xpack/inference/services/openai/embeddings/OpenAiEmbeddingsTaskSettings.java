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
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.USER;

/**
 * Defines the task settings for the openai service.
 *
 * User is an optional unique identifier representing the end-user, which can help OpenAI to monitor and detect abuse
 *  <a href="https://platform.openai.com/docs/api-reference/embeddings/create">see the openai docs for more details</a>
 */
public class OpenAiEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "openai_embeddings_task_settings";

    public static OpenAiEmbeddingsTaskSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String user = extractOptionalString(map, USER, ModelConfigurations.TASK_SETTINGS, validationException);
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiEmbeddingsTaskSettings(user);
    }

    /**
     * Creates a new {@link OpenAiEmbeddingsTaskSettings} object by overriding the values in originalSettings with the ones
     * passed in via requestSettings if the fields are not null.
     * @param originalSettings the original task settings from the inference entity configuration from storage
     * @param requestSettings the task settings from the request
     * @return a new {@link OpenAiEmbeddingsTaskSettings}
     */
    public static OpenAiEmbeddingsTaskSettings of(
        OpenAiEmbeddingsTaskSettings originalSettings,
        OpenAiEmbeddingsRequestTaskSettings requestSettings
    ) {
        var userToUse = requestSettings.user() == null ? originalSettings.user : requestSettings.user();
        return new OpenAiEmbeddingsTaskSettings(userToUse);
    }

    private final String user;

    public OpenAiEmbeddingsTaskSettings(@Nullable String user) {
        this.user = user;
    }

    @Override
    public boolean isEmpty() {
        return user == null;
    }

    public OpenAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            this.user = in.readOptionalString();
        } else {
            var discard = in.readString();
            this.user = in.readOptionalString();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (user != null) {
            builder.field(USER, user);
        }
        builder.endObject();
        return builder;
    }

    public String user() {
        return user;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeOptionalString(user);
        } else {
            out.writeString("m"); // write any string
            out.writeOptionalString(user);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenAiEmbeddingsTaskSettings that = (OpenAiEmbeddingsTaskSettings) o;
        return Objects.equals(user, that.user);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        OpenAiEmbeddingsRequestTaskSettings requestSettings = OpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(newSettings));
        return of(this, requestSettings);
    }
}
