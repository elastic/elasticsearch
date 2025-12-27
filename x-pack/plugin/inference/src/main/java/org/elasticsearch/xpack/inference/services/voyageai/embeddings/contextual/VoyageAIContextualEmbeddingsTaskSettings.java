/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual;

import org.elasticsearch.TransportVersion;
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
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAIService.VALID_INPUT_TYPE_VALUES;

/**
 * Defines the task settings for the voyageai contextualized embeddings service.
 *
 * <p>
 * <a href="https://docs.voyageai.com/reference/contextualized-embeddings-api">See api docs for details.</a>
 * </p>
 */
public class VoyageAIContextualEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "voyageai_contextual_embeddings_task_settings";
    public static final VoyageAIContextualEmbeddingsTaskSettings EMPTY_SETTINGS = new VoyageAIContextualEmbeddingsTaskSettings((InputType) null);
    static final String INPUT_TYPE = "input_type";
    private static final TransportVersion VOYAGE_AI_INTEGRATION_ADDED = TransportVersion.fromName("voyage_ai_integration_added");

    public static VoyageAIContextualEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
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

        return new VoyageAIContextualEmbeddingsTaskSettings(inputType);
    }

    /**
     * Creates a new {@link VoyageAIContextualEmbeddingsTaskSettings} by preferring non-null fields from the provided parameters.
     * @param originalSettings the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link VoyageAIContextualEmbeddingsTaskSettings}
     */
    public static VoyageAIContextualEmbeddingsTaskSettings of(
        VoyageAIContextualEmbeddingsTaskSettings originalSettings,
        VoyageAIContextualEmbeddingsTaskSettings requestTaskSettings
    ) {
        var inputTypeToUse = getValidInputType(originalSettings, requestTaskSettings);
        return new VoyageAIContextualEmbeddingsTaskSettings(inputTypeToUse);
    }

    private static InputType getValidInputType(
        VoyageAIContextualEmbeddingsTaskSettings originalSettings,
        VoyageAIContextualEmbeddingsTaskSettings requestTaskSettings
    ) {
        InputType inputTypeToUse = originalSettings.inputType;

        if (requestTaskSettings.inputType != null) {
            inputTypeToUse = requestTaskSettings.inputType;
        }

        return inputTypeToUse;
    }

    private final InputType inputType;

    public VoyageAIContextualEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalEnum(InputType.class));
    }

    public VoyageAIContextualEmbeddingsTaskSettings(@Nullable InputType inputType) {
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
        assert false : "should never be called when supportsVersion is used";
        return VOYAGE_AI_INTEGRATION_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(VOYAGE_AI_INTEGRATION_ADDED);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(inputType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VoyageAIContextualEmbeddingsTaskSettings that = (VoyageAIContextualEmbeddingsTaskSettings) o;
        return Objects.equals(inputType, that.inputType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputType);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        VoyageAIContextualEmbeddingsTaskSettings updatedSettings = VoyageAIContextualEmbeddingsTaskSettings.fromMap(new HashMap<>(newSettings));
        return of(this, updatedSettings);
    }
}
