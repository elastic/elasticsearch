/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.USER;

/**
 * Defines the task settings for the openai service.
 *
 * User is an optional unique identifier representing the end-user, which can help OpenAI to monitor and detect abuse
 *  <a href="https://platform.openai.com/docs/api-reference/embeddings/create">see the openai docs for more details</a>
 */
public class AzureOpenAiEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "azure_openai_embeddings_task_settings";

    public static AzureOpenAiEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String user = extractOptionalString(map, USER, ModelConfigurations.TASK_SETTINGS, validationException);
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureOpenAiEmbeddingsTaskSettings(user);
    }

    /**
     * Creates a new {@link AzureOpenAiEmbeddingsTaskSettings} object by overriding the values in originalSettings with the ones
     * passed in via requestSettings if the fields are not null.
     * @param originalSettings the original {@link AzureOpenAiEmbeddingsTaskSettings} from the inference entity configuration from storage
     * @param requestSettings the {@link AzureOpenAiEmbeddingsTaskSettings} from the request
     * @return a new {@link AzureOpenAiEmbeddingsTaskSettings}
     */
    public static AzureOpenAiEmbeddingsTaskSettings of(
        AzureOpenAiEmbeddingsTaskSettings originalSettings,
        AzureOpenAiEmbeddingsRequestTaskSettings requestSettings
    ) {
        var userToUse = requestSettings.user() == null ? originalSettings.user : requestSettings.user();
        return new AzureOpenAiEmbeddingsTaskSettings(userToUse);
    }

    private final String user;

    public AzureOpenAiEmbeddingsTaskSettings(@Nullable String user) {
        this.user = user;
    }

    public AzureOpenAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this.user = in.readOptionalString();
    }

    @Override
    public boolean isEmpty() {
        return user == null || user.isEmpty();
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
        return TransportVersions.V_8_14_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(user);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureOpenAiEmbeddingsTaskSettings that = (AzureOpenAiEmbeddingsTaskSettings) o;
        return Objects.equals(user, that.user);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        AzureOpenAiEmbeddingsRequestTaskSettings requestSettings = AzureOpenAiEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(newSettings)
        );
        return of(this, requestSettings);
    }
}
