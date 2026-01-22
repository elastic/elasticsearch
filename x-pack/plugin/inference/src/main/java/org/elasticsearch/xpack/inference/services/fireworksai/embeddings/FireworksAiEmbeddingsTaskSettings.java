/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiService.FIREWORKS_AI_SERVICE;

/**
 * Defines the task settings for FireworksAI embeddings.
 * Currently no task-level settings are supported - dimensions are only configurable in service settings.
 */
public class FireworksAiEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "fireworksai_embeddings_task_settings";

    public static final FireworksAiEmbeddingsTaskSettings EMPTY_SETTINGS = new FireworksAiEmbeddingsTaskSettings();

    public static FireworksAiEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        // No task settings to parse - dimensions are only in service settings
        return EMPTY_SETTINGS;
    }

    /**
     * Creates a new {@link FireworksAiEmbeddingsTaskSettings} by preferring non-null fields from the request settings
     * over the original settings.
     *
     * @param originalSettings the settings stored as part of the inference entity configuration
     * @param requestSettings  the settings passed in within the task_settings field of the request
     * @return a new {@link FireworksAiEmbeddingsTaskSettings}
     */
    public static FireworksAiEmbeddingsTaskSettings of(
        FireworksAiEmbeddingsTaskSettings originalSettings,
        FireworksAiEmbeddingsTaskSettings requestSettings
    ) {
        return EMPTY_SETTINGS;
    }

    public FireworksAiEmbeddingsTaskSettings() {}

    public FireworksAiEmbeddingsTaskSettings(StreamInput in) {
        this();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No fields to write
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return FIREWORKS_AI_SERVICE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return EMPTY_SETTINGS;
    }
}
