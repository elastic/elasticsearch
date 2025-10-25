/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

/**
 * Defines the task settings for FireworksAI embeddings.
 * Supports optional dimensions parameter for variable-length embeddings.
 */
public class FireworksAiEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "fireworksai_embeddings_task_settings";
    public static final String DIMENSIONS = "dimensions";

    public static final FireworksAiEmbeddingsTaskSettings EMPTY_SETTINGS = new FireworksAiEmbeddingsTaskSettings((Integer) null);

    public static FireworksAiEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        Integer dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new FireworksAiEmbeddingsTaskSettings(dimensions);
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
        return new FireworksAiEmbeddingsTaskSettings(
            requestSettings.dimensions != null ? requestSettings.dimensions : originalSettings.dimensions
        );
    }

    private final Integer dimensions;

    public FireworksAiEmbeddingsTaskSettings(@Nullable Integer dimensions) {
        this.dimensions = dimensions;
    }

    public FireworksAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this.dimensions = in.readOptionalInt();
    }

    @Nullable
    public Integer dimensions() {
        return dimensions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(dimensions);
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        FireworksAiEmbeddingsTaskSettings that = (FireworksAiEmbeddingsTaskSettings) object;
        return Objects.equals(dimensions, that.dimensions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimensions);
    }

    @Override
    public boolean isEmpty() {
        return dimensions == null;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return fromMap(newSettings);
    }
}
