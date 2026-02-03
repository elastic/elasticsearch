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
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadService;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;

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

        InputType inputType = extractOptionalEnum(
            map,
            MixedbreadUtils.INPUT_TYPE_FIELD,
            ModelConfigurations.TASK_SETTINGS,
            InputType::fromString,
            MixedbreadService.VALID_INPUT_TYPE_VALUES,
            validationException
        );
        Truncation truncation = extractOptionalEnum(
            map,
            MixedbreadUtils.TRUNCATE_FIELD,
            ModelConfigurations.TASK_SETTINGS,
            Truncation::fromString,
            Truncation.ALL,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new MixedbreadEmbeddingsTaskSettings(inputType, truncation);
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
            requestTaskSettings.getInputType() != null ? requestTaskSettings.getInputType() : originalSettings.getInputType(),
            requestTaskSettings.getTruncation() != null ? requestTaskSettings.getTruncation() : originalSettings.getTruncation()
        );
    }

    private final InputType inputType;
    private final Truncation truncation;

    public MixedbreadEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalEnum(InputType.class), in.readOptionalEnum(Truncation.class));
    }

    public MixedbreadEmbeddingsTaskSettings(@Nullable InputType inputType, @Nullable Truncation truncation) {
        validateInputType(inputType);
        this.inputType = inputType;
        this.truncation = truncation;
    }

    private void validateInputType(InputType inputType) {
        if (inputType == null) {
            return;
        }
        assert MixedbreadService.VALID_INPUT_TYPE_VALUES.contains(inputType) : invalidInputTypeMessage(inputType);
    }

    @Override
    public boolean isEmpty() {
        return inputType == null && truncation == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inputType != null) {
            builder.field(MixedbreadUtils.INPUT_TYPE_FIELD, inputType);
        }
        if (truncation != null) {
            builder.field(MixedbreadUtils.TRUNCATE_FIELD, truncation);
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
        out.writeOptionalEnum(inputType);
        out.writeOptionalEnum(truncation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MixedbreadEmbeddingsTaskSettings that = (MixedbreadEmbeddingsTaskSettings) o;
        return Objects.equals(inputType, that.inputType) && Objects.equals(truncation, that.truncation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputType, truncation);
    }

    public InputType getInputType() {
        return inputType;
    }

    public Truncation getTruncation() {
        return truncation;
    }

    @Override
    public MixedbreadEmbeddingsTaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        MixedbreadEmbeddingsTaskSettings updatedSettings = MixedbreadEmbeddingsTaskSettings.fromMap(new HashMap<>(newSettings));
        return of(this, updatedSettings);
    }
}
