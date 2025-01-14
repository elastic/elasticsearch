/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Defines the task settings for the AlibabaCloudSearch completion service.
 *
 * <p>
 * <a href="https://help.aliyun.com/zh/open-search/search-platform/developer-reference/text-generation-api-details">
 *     See api docs for details.</a>
 * </p>
 */
public class AlibabaCloudSearchCompletionTaskSettings implements TaskSettings {
    public static final String NAME = "alibabacloud_search_completion_task_settings";
    public static final String PARAMETERS = "parameters";

    static final AlibabaCloudSearchCompletionTaskSettings EMPTY_SETTINGS = new AlibabaCloudSearchCompletionTaskSettings(
        (Map<String, Object>) null
    );

    @SuppressWarnings("unchecked")
    public static AlibabaCloudSearchCompletionTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        Map<String, Object> parameters = ServiceUtils.removeAsType(map, PARAMETERS, Map.class, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return of(parameters);
    }

    /**
     * Creates a new {@link AlibabaCloudSearchCompletionTaskSettings}
     * by preferring non-null fields from the request settings over the original settings.
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link AlibabaCloudSearchCompletionTaskSettings}
     */
    public static AlibabaCloudSearchCompletionTaskSettings of(
        AlibabaCloudSearchCompletionTaskSettings originalSettings,
        AlibabaCloudSearchCompletionTaskSettings requestTaskSettings
    ) {
        if (originalSettings != null
            && originalSettings.parameters != null
            && requestTaskSettings != null
            && requestTaskSettings.parameters != null) {
            var copy = new HashMap<>(originalSettings.parameters);
            requestTaskSettings.parameters.forEach((key, value) -> copy.merge(key, value, (originalValue, requestValue) -> requestValue));
            return new AlibabaCloudSearchCompletionTaskSettings(copy);
        } else {
            return new AlibabaCloudSearchCompletionTaskSettings(
                requestTaskSettings.getParameters() != null ? requestTaskSettings.getParameters() : originalSettings.getParameters()
            );
        }
    }

    public static AlibabaCloudSearchCompletionTaskSettings of(Map<String, Object> parameters) {
        return new AlibabaCloudSearchCompletionTaskSettings(parameters);
    }

    private final Map<String, Object> parameters;

    public AlibabaCloudSearchCompletionTaskSettings(StreamInput in) throws IOException {
        this(in.readGenericMap());
    }

    public AlibabaCloudSearchCompletionTaskSettings(@Nullable Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    @Override
    public boolean isEmpty() {
        return parameters == null || parameters.isEmpty();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (parameters != null && parameters.isEmpty() == false) {
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
        return TransportVersions.V_8_16_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(parameters);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlibabaCloudSearchCompletionTaskSettings that = (AlibabaCloudSearchCompletionTaskSettings) o;
        return Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameters);
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        AlibabaCloudSearchCompletionTaskSettings updatedSettings = AlibabaCloudSearchCompletionTaskSettings.fromMap(
            new HashMap<>(newSettings)
        );
        return of(this, updatedSettings);
    }
}
