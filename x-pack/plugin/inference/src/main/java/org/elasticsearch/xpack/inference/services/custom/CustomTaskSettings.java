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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeNullValues;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.validateMapValues;

public class CustomTaskSettings implements TaskSettings {
    public static final String NAME = "custom_task_settings";

    public static final String PARAMETERS = "parameters";

    static final CustomTaskSettings EMPTY_SETTINGS = new CustomTaskSettings(new HashMap<>());

    public static CustomTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        Map<String, Object> parameters = extractOptionalMap(map, PARAMETERS, ModelConfigurations.TASK_SETTINGS, validationException);
        removeNullValues(parameters);
        validateMapValues(
            parameters,
            List.of(String.class, Integer.class, Double.class, Float.class, Boolean.class),
            PARAMETERS,
            validationException,
            false
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CustomTaskSettings(Objects.requireNonNullElse(parameters, new HashMap<>()));
    }

    /**
     * Creates a new {@link CustomTaskSettings}
     * by preferring non-null fields from the request settings over the original settings.
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link CustomTaskSettings}
     */
    public static CustomTaskSettings of(CustomTaskSettings originalSettings, CustomTaskSettings requestTaskSettings) {
        var copy = new HashMap<>(originalSettings.parameters);
        requestTaskSettings.parameters.forEach((key, value) -> copy.merge(key, value, (originalValue, requestValue) -> requestValue));
        return new CustomTaskSettings(copy);
    }

    private final Map<String, Object> parameters;

    public CustomTaskSettings(StreamInput in) throws IOException {
        parameters = in.readGenericMap();
    }

    public CustomTaskSettings(Map<String, Object> parameters) {
        this.parameters = Objects.requireNonNull(parameters);
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (parameters.isEmpty() == false) {
            builder.field(PARAMETERS, parameters);
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
        return TransportVersions.INFERENCE_CUSTOM_SERVICE_ADDED_8_19;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(parameters);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomTaskSettings that = (CustomTaskSettings) o;
        return Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameters);
    }

    @Override
    public boolean isEmpty() {
        return parameters.isEmpty();
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        CustomTaskSettings updatedSettings = CustomTaskSettings.fromMap(new HashMap<>(newSettings));
        return of(this, updatedSettings);
    }
}
