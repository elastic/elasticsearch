/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.USER_FIELD;

public class AzureAiStudioEmbeddingsTaskSettings implements TaskSettings {
    public static final String NAME = "azure_ai_studio_embeddings_task_settings";

    public static AzureAiStudioEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String user = extractOptionalString(map, USER_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureAiStudioEmbeddingsTaskSettings(user);
    }

    /**
     * Creates a new {@link AzureOpenAiEmbeddingsTaskSettings} object by overriding the values in originalSettings with the ones
     * passed in via requestSettings if the fields are not null.
     *
     * @param originalSettings the original {@link AzureOpenAiEmbeddingsTaskSettings} from the inference entity configuration from storage
     * @param requestSettings  the {@link AzureOpenAiEmbeddingsTaskSettings} from the request
     * @return a new {@link AzureOpenAiEmbeddingsTaskSettings}
     */
    public static AzureAiStudioEmbeddingsTaskSettings of(
        AzureAiStudioEmbeddingsTaskSettings originalSettings,
        AzureAiStudioEmbeddingsRequestTaskSettings requestSettings
    ) {
        var userToUse = requestSettings.user() == null ? originalSettings.user : requestSettings.user();
        return new AzureAiStudioEmbeddingsTaskSettings(userToUse);
    }

    public AzureAiStudioEmbeddingsTaskSettings(@Nullable String user) {
        this.user = user;
    }

    public AzureAiStudioEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this.user = in.readOptionalString();
    }

    private final String user;

    public String user() {
        return this.user;
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
        out.writeOptionalString(this.user);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (user != null) {
            builder.field(USER_FIELD, user);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureAiStudioEmbeddingsTaskSettings that = (AzureAiStudioEmbeddingsTaskSettings) o;
        return Objects.equals(user, that.user);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(user);
    }
}
