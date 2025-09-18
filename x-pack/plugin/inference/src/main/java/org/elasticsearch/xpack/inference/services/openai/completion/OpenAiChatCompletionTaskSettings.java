/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

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

import static org.elasticsearch.TransportVersions.INFERENCE_API_OPENAI_HEADERS;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalMapRemoveNulls;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.validateMapStringValues;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.HEADERS;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.USER;

public class OpenAiChatCompletionTaskSettings implements TaskSettings {

    public static final String NAME = "openai_completion_task_settings";

    public static OpenAiChatCompletionTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String user = extractOptionalString(map, USER, ModelConfigurations.TASK_SETTINGS, validationException);
        var headers = extractOptionalMapRemoveNulls(map, HEADERS, validationException);
        var stringHeaders = validateMapStringValues(headers, HEADERS, validationException, false, null);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiChatCompletionTaskSettings(user, stringHeaders);
    }

    private final String user;
    @Nullable
    private final Map<String, String> headers;

    public OpenAiChatCompletionTaskSettings(@Nullable String user, @Nullable Map<String, String> headers) {
        this.user = user;
        this.headers = headers;
    }

    public OpenAiChatCompletionTaskSettings(StreamInput in) throws IOException {
        this.user = in.readOptionalString();

        if (in.getTransportVersion().onOrAfter(INFERENCE_API_OPENAI_HEADERS)) {
            headers = in.readOptionalImmutableMap(StreamInput::readString, StreamInput::readString);
        } else {
            headers = null;
        }
    }

    @Override
    public boolean isEmpty() {
        return user == null && (headers == null || headers.isEmpty());
    }

    public static OpenAiChatCompletionTaskSettings of(
        OpenAiChatCompletionTaskSettings originalSettings,
        OpenAiChatCompletionRequestTaskSettings requestSettings
    ) {
        var userToUse = requestSettings.user() == null ? originalSettings.user : requestSettings.user();
        var headersToUse = requestSettings.headers() == null ? originalSettings.headers : requestSettings.headers();
        return new OpenAiChatCompletionTaskSettings(userToUse, headersToUse);
    }

    public String user() {
        return user;
    }

    public Map<String, String> headers() {
        return headers;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (user != null) {
            builder.field(USER, user);
        }

        if (headers != null && headers.isEmpty() == false) {
            builder.field(HEADERS, headers);
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
        return TransportVersions.V_8_14_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(user);
        if (out.getTransportVersion().onOrAfter(INFERENCE_API_OPENAI_HEADERS)) {
            out.writeOptionalMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        OpenAiChatCompletionTaskSettings that = (OpenAiChatCompletionTaskSettings) object;
        return Objects.equals(user, that.user) && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, headers);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        OpenAiChatCompletionRequestTaskSettings updatedSettings = OpenAiChatCompletionRequestTaskSettings.fromMap(
            new HashMap<>(newSettings)
        );
        return of(this, updatedSettings);
    }
}
