/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

/**
 * Defines the task settings for the Mixedbread text embeddings service.
 */
public class MixedbreadEmbeddingsTaskSettings implements TaskSettings {
    public static final String NAME = "mixedbread_embeddings_task_settings";

    public static final MixedbreadEmbeddingsTaskSettings EMPTY_SETTINGS = new MixedbreadEmbeddingsTaskSettings(null, null);

    /**
     * Creates a new instance of {@link MixedbreadEmbeddingsTaskSettings} from a map of settings.
     *
     * @param map the map of settings
     * @return a constructed {@link MixedbreadEmbeddingsTaskSettings}
     */
    public static MixedbreadEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        var prompt = extractOptionalString(map, MixedbreadUtils.PROMPT_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);
        var normalized = extractOptionalBoolean(map, MixedbreadUtils.NORMALIZED_FIELD, validationException);

        validationException.throwIfValidationErrorsExist();

        return new MixedbreadEmbeddingsTaskSettings(prompt, normalized);
    }

    /**
     * Creates a new {@link MixedbreadEmbeddingsTaskSettings}
     * by preferring non-null fields from the request settings over the original settings.
     *
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link MixedbreadEmbeddingsTaskSettings}
     */
    public static MixedbreadEmbeddingsTaskSettings of(
        MixedbreadEmbeddingsTaskSettings originalSettings,
        MixedbreadEmbeddingsTaskSettings requestTaskSettings
    ) {
        if (requestTaskSettings.isEmpty() || originalSettings.equals(requestTaskSettings)) {
            return originalSettings;
        }
        return new MixedbreadEmbeddingsTaskSettings(
            requestTaskSettings.getPrompt() != null ? requestTaskSettings.getPrompt() : originalSettings.getPrompt(),
            requestTaskSettings.getNormalized() != null ? requestTaskSettings.getNormalized() : originalSettings.getNormalized()
        );
    }

    private final String prompt;
    private final Boolean normalized;

    public MixedbreadEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readOptionalBoolean());
    }

    public MixedbreadEmbeddingsTaskSettings(@Nullable String prompt, @Nullable Boolean normalized) {
        this.prompt = prompt;
        this.normalized = normalized;
    }

    @Override
    public boolean isEmpty() {
        return prompt == null && normalized == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (prompt != null) {
            builder.field(MixedbreadUtils.PROMPT_FIELD, prompt);
        }
        if (normalized != null) {
            builder.field(MixedbreadUtils.NORMALIZED_FIELD, normalized);
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
        assert false : "should never be called when supportsVersion is used";
        return MixedbreadUtils.INFERENCE_MIXEDBREAD_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return MixedbreadUtils.supportsMixedbread(version);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(prompt);
        out.writeOptionalBoolean(normalized);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MixedbreadEmbeddingsTaskSettings that = (MixedbreadEmbeddingsTaskSettings) o;
        return Objects.equals(prompt, that.prompt) && Objects.equals(normalized, that.normalized);
    }

    @Override
    public int hashCode() {
        return Objects.hash(prompt, normalized);
    }

    public String getPrompt() {
        return prompt;
    }

    public Boolean getNormalized() {
        return normalized;
    }

    @Override
    public MixedbreadEmbeddingsTaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        MixedbreadEmbeddingsTaskSettings updatedSettings = MixedbreadEmbeddingsTaskSettings.fromMap(new HashMap<>(newSettings));
        return of(this, updatedSettings);
    }
}
