/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

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

public class AzureOpenAiCompletionTaskSettings implements TaskSettings {

    public static final String NAME = "azure_openai_completion_task_settings";

    public static final String USER = "user";

    public static AzureOpenAiCompletionTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String user = extractOptionalString(map, USER, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureOpenAiCompletionTaskSettings(user);
    }

    private final String user;

    public static AzureOpenAiCompletionTaskSettings of(
        AzureOpenAiCompletionTaskSettings originalSettings,
        AzureOpenAiCompletionRequestTaskSettings requestSettings
    ) {
        var userToUse = requestSettings.user() == null ? originalSettings.user : requestSettings.user();
        return new AzureOpenAiCompletionTaskSettings(userToUse);
    }

    public AzureOpenAiCompletionTaskSettings(@Nullable String user) {
        this.user = user;
    }

    public AzureOpenAiCompletionTaskSettings(StreamInput in) throws IOException {
        this.user = in.readOptionalString();
    }

    @Override
    public boolean isEmpty() {
        return user == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (user != null) {
                builder.field(USER, user);
            }
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
        return TransportVersions.V_8_15_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(user);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        AzureOpenAiCompletionTaskSettings that = (AzureOpenAiCompletionTaskSettings) object;
        return Objects.equals(user, that.user);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        AzureOpenAiCompletionRequestTaskSettings updatedSettings = AzureOpenAiCompletionRequestTaskSettings.fromMap(
            new HashMap<>(newSettings)
        );
        return of(this, updatedSettings);
    }
}
