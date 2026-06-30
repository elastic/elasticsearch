/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceChatCompletionTaskSettings;

import java.io.IOException;
import java.util.Map;

/**
 * This class defines an empty task settings object that cannot be updated. If the {@link #updatedTaskSettings} is called with a
 * non-empty map, it will throw an exception.
 */
public record ImmutableEmptyTaskSettings() implements TaskSettings {
    public static final String NAME = "immutable_empty_task_settings";

    public static final ImmutableEmptyTaskSettings INSTANCE = new ImmutableEmptyTaskSettings();

    public static ImmutableEmptyTaskSettings fromMap(Map<String, Object> settings, ConfigurationParseContext context) {
        if (settings.isEmpty() || context == ConfigurationParseContext.PERSISTENT) {
            return INSTANCE;
        }

        throw new ElasticsearchStatusException(
            "[{}] Configuration contains unknown settings {}",
            RestStatus.BAD_REQUEST,
            ModelConfigurations.TASK_SETTINGS,
            settings.keySet()
        );
    }

    public ImmutableEmptyTaskSettings(StreamInput in) {
        this();
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ElasticInferenceServiceChatCompletionTaskSettings.EIS_REASONING_TASK_SETTINGS_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return fromMap(newSettings, ConfigurationParseContext.REQUEST);
    }
}
