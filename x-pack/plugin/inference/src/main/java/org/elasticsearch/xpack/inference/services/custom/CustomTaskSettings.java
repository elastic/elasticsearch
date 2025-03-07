/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalMap;

public class CustomTaskSettings implements TaskSettings {
    public static final String NAME = "custom_task_settings";

    public static final String PARAMETERS = "parameters";
    public static final String IGNORE_PLACEHOLDER_CHECK = "ignore_placeholder_check";

    static final CustomTaskSettings EMPTY_SETTINGS = new CustomTaskSettings(null, null);

    public static CustomTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        Map<String, Object> parameters = extractOptionalMap(map, PARAMETERS, ModelConfigurations.TASK_SETTINGS, validationException);
        Boolean ignorePlaceholderCheck = extractOptionalBoolean(map, IGNORE_PLACEHOLDER_CHECK, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CustomTaskSettings(parameters, ignorePlaceholderCheck);
    }

    /**
     * Creates a new {@link CustomTaskSettings}
     * by preferring non-null fields from the request settings over the original settings.
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link CustomTaskSettings}
     */

    public static CustomTaskSettings of(CustomTaskSettings originalSettings, CustomTaskSettings requestTaskSettings) {
        // If both requestTaskSettings.getParameters() and originalSettings.getParameters() are defined
        // the maps should be merged.
        if (originalSettings != null
            && originalSettings.parameters != null
            && requestTaskSettings != null
            && requestTaskSettings.parameters != null) {
            var copy = new HashMap<>(originalSettings.parameters);
            requestTaskSettings.parameters.forEach((key, value) -> copy.merge(key, value, (originalValue, requestValue) -> requestValue));
            Boolean ignorePlaceholderCheck = requestTaskSettings.getIgnorePlaceholderCheck() != null
                ? requestTaskSettings.getIgnorePlaceholderCheck()
                : originalSettings.getIgnorePlaceholderCheck();
            return new CustomTaskSettings(copy, ignorePlaceholderCheck);
        } else {
            return new CustomTaskSettings(
                requestTaskSettings.getParameters() != null ? requestTaskSettings.getParameters() : originalSettings.getParameters(),
                requestTaskSettings.getIgnorePlaceholderCheck() != null
                    ? requestTaskSettings.getIgnorePlaceholderCheck()
                    : originalSettings.getIgnorePlaceholderCheck()
            );
        }
    }

    private final Map<String, Object> parameters;
    private Boolean ignorePlaceholderCheck;

    public CustomTaskSettings(StreamInput in) throws IOException {
        parameters = in.readBoolean() ? in.readGenericMap() : null;
        ignorePlaceholderCheck = in.readOptionalBoolean();
    }

    public CustomTaskSettings(Map<String, Object> parameters, Boolean ignorePlaceholderCheck) {
        this.parameters = parameters;
        this.ignorePlaceholderCheck = ignorePlaceholderCheck;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public Boolean getIgnorePlaceholderCheck() {
        return ignorePlaceholderCheck;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (parameters != null) {
            builder.field(PARAMETERS, parameters);
        }
        if (ignorePlaceholderCheck != null) {
            builder.field(IGNORE_PLACEHOLDER_CHECK, ignorePlaceholderCheck);
        }
        builder.endObject();
        return builder;
    }

    public Map<String, Object> getParametersOrDefault() {
        return parameters;
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
        if (parameters == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeGenericMap(parameters);
        }
        out.writeOptionalBoolean(ignorePlaceholderCheck);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomTaskSettings that = (CustomTaskSettings) o;
        return Objects.equals(parameters, that.parameters) && Objects.equals(ignorePlaceholderCheck, that.ignorePlaceholderCheck);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameters, ignorePlaceholderCheck);
    }

    @Override
    public boolean isEmpty() {
        return (parameters == null || parameters.isEmpty()) && ignorePlaceholderCheck == null;
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        CustomTaskSettings updatedSettings = CustomTaskSettings.fromMap(new HashMap<>(newSettings));
        return of(this, updatedSettings);
    }
}
