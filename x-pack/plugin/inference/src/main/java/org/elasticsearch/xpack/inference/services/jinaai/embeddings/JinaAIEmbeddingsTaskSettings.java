/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIService.VALID_INPUT_TYPE_VALUES;

/**
 * Defines the task settings for the JinaAI text embeddings service.
 *
 */
public class JinaAIEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "jinaai_embeddings_task_settings";
    public static final JinaAIEmbeddingsTaskSettings EMPTY_SETTINGS = new JinaAIEmbeddingsTaskSettings((InputType) null);
    static final String INPUT_TYPE = "input_type";

    public static JinaAIEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        InputType inputType = extractOptionalEnum(
            map,
            INPUT_TYPE,
            ModelConfigurations.TASK_SETTINGS,
            InputType::fromString,
            VALID_INPUT_TYPE_VALUES,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new JinaAIEmbeddingsTaskSettings(inputType);
    }

    /**
     * Creates a new {@link JinaAIEmbeddingsTaskSettings} by preferring non-null fields from the provided parameters.
     * For the input type, preference is given to requestInputType if it is not null and not UNSPECIFIED.
     * Then preference is given to the requestTaskSettings and finally to originalSettings even if the value is null.
     *
     * Similarly, for the truncation field preference is given to requestTaskSettings if it is not null and then to
     * originalSettings.
     * @param originalSettings the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link JinaAIEmbeddingsTaskSettings}
     */
    public static JinaAIEmbeddingsTaskSettings of(
        JinaAIEmbeddingsTaskSettings originalSettings,
        JinaAIEmbeddingsTaskSettings requestTaskSettings
    ) {
        var inputTypeToUse = getValidInputType(originalSettings, requestTaskSettings);

        return new JinaAIEmbeddingsTaskSettings(inputTypeToUse);
    }

    private static InputType getValidInputType(
        JinaAIEmbeddingsTaskSettings originalSettings,
        JinaAIEmbeddingsTaskSettings requestTaskSettings
    ) {
        InputType inputTypeToUse = originalSettings.inputType;

        if (requestTaskSettings.inputType != null) {
            inputTypeToUse = requestTaskSettings.inputType;
        }

        return inputTypeToUse;
    }

    private final InputType inputType;

    public JinaAIEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalEnum(InputType.class));
    }

    public JinaAIEmbeddingsTaskSettings(@Nullable InputType inputType) {
        validateInputType(inputType);
        this.inputType = inputType;
    }

    private static void validateInputType(InputType inputType) {
        if (inputType == null) {
            return;
        }

        assert VALID_INPUT_TYPE_VALUES.contains(inputType) : invalidInputTypeMessage(inputType);
    }

    @Override
    public boolean isEmpty() {
        return inputType == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inputType != null) {
            builder.field(INPUT_TYPE, inputType);
        }

        builder.endObject();
        return builder;
    }

    public InputType getInputType() {
        return inputType;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.JINA_AI_INTEGRATION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(inputType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JinaAIEmbeddingsTaskSettings that = (JinaAIEmbeddingsTaskSettings) o;
        return Objects.equals(inputType, that.inputType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputType);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        JinaAIEmbeddingsTaskSettings updatedSettings = JinaAIEmbeddingsTaskSettings.fromMap(new HashMap<>(newSettings));
        return of(this, updatedSettings);
    }
}
