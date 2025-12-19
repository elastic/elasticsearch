/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.embeddings;

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
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaService.VALID_INPUT_TYPE_VALUES;
import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceFields.INPUT_TYPE_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceFields.TRUNCATE_FIELD_NAME;

/**
 * Defines the task settings for the Nvidia text embeddings service.
 */
public class NvidiaEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "nvidia_embeddings_task_settings";
    public static final NvidiaEmbeddingsTaskSettings EMPTY_SETTINGS = new NvidiaEmbeddingsTaskSettings(null, null);

    private final InputType inputType;
    private final Truncation truncation;

    /**
     * Creates a new instance of {@link NvidiaEmbeddingsTaskSettings} from a map of settings.
     *
     * @param map the map of settings
     * @return a constructed {@link NvidiaEmbeddingsTaskSettings}
     */
    public static NvidiaEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        InputType inputType = extractOptionalEnum(
            map,
            INPUT_TYPE_FIELD_NAME,
            ModelConfigurations.TASK_SETTINGS,
            InputType::fromString,
            VALID_INPUT_TYPE_VALUES,
            validationException
        );
        Truncation truncation = extractOptionalEnum(
            map,
            TRUNCATE_FIELD_NAME,
            ModelConfigurations.TASK_SETTINGS,
            Truncation::fromString,
            Truncation.ALL,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new NvidiaEmbeddingsTaskSettings(inputType, truncation);
    }

    /**
     * Creates a new instance of {@link NvidiaEmbeddingsTaskSettings}
     * by using non-null fields from the request settings over the original settings.
     *
     * @param originalSettings the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link NvidiaEmbeddingsTaskSettings}
     */
    public static NvidiaEmbeddingsTaskSettings of(
        NvidiaEmbeddingsTaskSettings originalSettings,
        NvidiaEmbeddingsTaskSettings requestTaskSettings
    ) {
        if (requestTaskSettings.isEmpty() || originalSettings.equals(requestTaskSettings)) {
            return originalSettings;
        }
        var inputTypeToUse = extractInputTypeToUse(originalSettings, requestTaskSettings);
        var truncationToUse = extractTruncationToUse(originalSettings, requestTaskSettings);

        return new NvidiaEmbeddingsTaskSettings(inputTypeToUse, truncationToUse);
    }

    private static InputType extractInputTypeToUse(
        NvidiaEmbeddingsTaskSettings originalSettings,
        NvidiaEmbeddingsTaskSettings requestTaskSettings
    ) {
        if (requestTaskSettings.getInputType() == null) {
            return originalSettings.getInputType();
        }
        return requestTaskSettings.getInputType();
    }

    private static Truncation extractTruncationToUse(
        NvidiaEmbeddingsTaskSettings originalSettings,
        NvidiaEmbeddingsTaskSettings requestTaskSettings
    ) {
        if (requestTaskSettings.getTruncation() == null) {
            return originalSettings.getTruncation();
        }
        return requestTaskSettings.getTruncation();
    }

    public NvidiaEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalEnum(InputType.class), in.readOptionalEnum(Truncation.class));
    }

    public NvidiaEmbeddingsTaskSettings(@Nullable InputType inputType, @Nullable Truncation truncation) {
        validateInputType(inputType);
        this.inputType = inputType;
        this.truncation = truncation;
    }

    private static void validateInputType(InputType inputType) {
        if (inputType == null) {
            return;
        }
        assert VALID_INPUT_TYPE_VALUES.contains(inputType) : invalidInputTypeMessage(inputType);
    }

    @Override
    public boolean isEmpty() {
        return inputType == null && truncation == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inputType != null) {
            builder.field(INPUT_TYPE_FIELD_NAME, inputType);
        }
        if (truncation != null) {
            builder.field(TRUNCATE_FIELD_NAME, truncation);
        }
        builder.endObject();
        return builder;
    }

    public InputType getInputType() {
        return inputType;
    }

    public Truncation getTruncation() {
        return truncation;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return NvidiaUtils.ML_INFERENCE_NVIDIA_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return NvidiaUtils.supportsNvidia(version);
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
        NvidiaEmbeddingsTaskSettings that = (NvidiaEmbeddingsTaskSettings) o;
        return Objects.equals(inputType, that.inputType) && Objects.equals(truncation, that.truncation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputType, truncation);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        NvidiaEmbeddingsTaskSettings updatedSettings = NvidiaEmbeddingsTaskSettings.fromMap(new HashMap<>(newSettings));
        return of(this, updatedSettings);
    }
}
