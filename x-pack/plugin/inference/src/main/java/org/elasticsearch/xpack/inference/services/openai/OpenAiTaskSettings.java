/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalMapRemoveNulls;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.validateMapStringValues;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.HEADERS;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.USER;

public abstract class OpenAiTaskSettings<T extends OpenAiTaskSettings<T>> implements TaskSettings {
    private static final Settings EMPTY_SETTINGS = new Settings(null, null);

    private final Settings taskSettings;

    public OpenAiTaskSettings(Map<String, Object> map) {
        this(fromMap(map));
    }

    public record Settings(@Nullable String user, @Nullable Map<String, String> headers) {}

    public static Settings createSettings(String user, Map<String, String> stringHeaders) {
        if (user == null && stringHeaders == null) {
            return EMPTY_SETTINGS;
        } else {
            return new Settings(user, stringHeaders);
        }
    }

    private static Settings fromMap(Map<String, Object> map) {
        if (map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        String user = extractOptionalString(map, USER, ModelConfigurations.TASK_SETTINGS, validationException);
        Map<String, Object> headers = extractOptionalMapRemoveNulls(map, HEADERS, validationException);
        var stringHeaders = validateMapStringValues(headers, HEADERS, validationException, false, null);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return createSettings(user, stringHeaders);
    }

    public OpenAiTaskSettings(@Nullable String user, @Nullable Map<String, String> headers) {
        this(new Settings(user, headers));
    }

    protected OpenAiTaskSettings(Settings taskSettings) {
        this.taskSettings = Objects.requireNonNull(taskSettings);
    }

    public String user() {
        return taskSettings.user();
    }

    public Map<String, String> headers() {
        return taskSettings.headers();
    }

    @Override
    public boolean isEmpty() {
        return taskSettings.user() == null && (taskSettings.headers() == null || taskSettings.headers().isEmpty());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (taskSettings.user() != null) {
            builder.field(USER, taskSettings.user());
        }

        if (taskSettings.headers() != null && taskSettings.headers().isEmpty() == false) {
            builder.field(HEADERS, taskSettings.headers());
        }

        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenAiTaskSettings<?> that = (OpenAiTaskSettings<?>) o;
        return Objects.equals(taskSettings, that.taskSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskSettings);
    }

    @Override
    public T updatedTaskSettings(Map<String, Object> newSettings) {
        Settings updatedSettings = fromMap(new HashMap<>(newSettings));

        var userToUse = updatedSettings.user() == null ? taskSettings.user() : updatedSettings.user();
        var headersToUse = updatedSettings.headers() == null ? taskSettings.headers() : updatedSettings.headers();
        return create(userToUse, headersToUse);
    }

    protected abstract T create(@Nullable String user, @Nullable Map<String, String> headers);

}
