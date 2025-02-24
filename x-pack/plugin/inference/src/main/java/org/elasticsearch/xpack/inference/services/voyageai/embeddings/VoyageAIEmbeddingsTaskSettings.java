/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceFields.TRUNCATION;

/**
 * Defines the task settings for the voyageai text embeddings service.
 *
 * <p>
 * <a href="https://docs.voyageai.com/docs/embeddings">See api docs for details.</a>
 * </p>
 */
public class VoyageAIEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "voyageai_embeddings_task_settings";
    public static final VoyageAIEmbeddingsTaskSettings EMPTY_SETTINGS = new VoyageAIEmbeddingsTaskSettings(null, null);
    static final String INPUT_TYPE = "input_type";
    static final EnumSet<InputType> VALID_REQUEST_VALUES = EnumSet.of(InputType.INGEST, InputType.SEARCH);

    public static VoyageAIEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        InputType inputType = extractOptionalEnum(
            map,
            INPUT_TYPE,
            ModelConfigurations.TASK_SETTINGS,
            InputType::fromString,
            VALID_REQUEST_VALUES,
            validationException
        );
        Boolean truncation = extractOptionalBoolean(map, TRUNCATION, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new VoyageAIEmbeddingsTaskSettings(inputType, truncation);
    }

    /**
     * Creates a new {@link VoyageAIEmbeddingsTaskSettings} by preferring non-null fields from the provided parameters.
     * For the input type, preference is given to requestInputType if it is not null and not UNSPECIFIED.
     * Then preference is given to the requestTaskSettings and finally to originalSettings even if the value is null.
     * Similarly, for the truncation field preference is given to requestTaskSettings if it is not null and then to
     * originalSettings.
     * @param originalSettings the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @param requestInputType the input type passed in the request parameters
     * @return a constructed {@link VoyageAIEmbeddingsTaskSettings}
     */
    public static VoyageAIEmbeddingsTaskSettings of(
        VoyageAIEmbeddingsTaskSettings originalSettings,
        VoyageAIEmbeddingsTaskSettings requestTaskSettings,
        InputType requestInputType
    ) {
        var inputTypeToUse = getValidInputType(originalSettings, requestTaskSettings, requestInputType);
        var truncationToUse = getValidTruncation(originalSettings, requestTaskSettings);

        return new VoyageAIEmbeddingsTaskSettings(inputTypeToUse, truncationToUse);
    }

    private static InputType getValidInputType(
        VoyageAIEmbeddingsTaskSettings originalSettings,
        VoyageAIEmbeddingsTaskSettings requestTaskSettings,
        InputType requestInputType
    ) {
        InputType inputTypeToUse = originalSettings.inputType;

        if (VALID_REQUEST_VALUES.contains(requestInputType)) {
            inputTypeToUse = requestInputType;
        } else if (requestTaskSettings.inputType != null) {
            inputTypeToUse = requestTaskSettings.inputType;
        }

        return inputTypeToUse;
    }

    private static Boolean getValidTruncation(
        VoyageAIEmbeddingsTaskSettings originalSettings,
        VoyageAIEmbeddingsTaskSettings requestTaskSettings
    ) {
        return requestTaskSettings.getTruncation() == null ? originalSettings.truncation : requestTaskSettings.getTruncation();
    }

    private final InputType inputType;
    private final Boolean truncation;

    public VoyageAIEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalEnum(InputType.class), in.readOptionalBoolean());
    }

    public VoyageAIEmbeddingsTaskSettings(@Nullable InputType inputType, @Nullable Boolean truncation) {
        validateInputType(inputType);
        this.inputType = inputType;
        this.truncation = truncation;
    }

    private static void validateInputType(InputType inputType) {
        if (inputType == null) {
            return;
        }

        assert VALID_REQUEST_VALUES.contains(inputType) : invalidInputTypeMessage(inputType);
    }

    @Override
    public boolean isEmpty() {
        return inputType == null && truncation == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inputType != null) {
            builder.field(INPUT_TYPE, inputType);
        }

        if (truncation != null) {
            builder.field(TRUNCATION, truncation);
        }

        builder.endObject();
        return builder;
    }

    public InputType getInputType() {
        return inputType;
    }

    public Boolean getTruncation() {
        return truncation;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.VOYAGE_AI_INTEGRATION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(inputType);
        out.writeOptionalBoolean(truncation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VoyageAIEmbeddingsTaskSettings that = (VoyageAIEmbeddingsTaskSettings) o;
        return Objects.equals(inputType, that.inputType) && Objects.equals(truncation, that.truncation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputType, truncation);
    }

    public static String invalidInputTypeMessage(InputType inputType) {
        return Strings.format("received invalid input type value [%s]", inputType.toString());
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        VoyageAIEmbeddingsTaskSettings updatedSettings = VoyageAIEmbeddingsTaskSettings.fromMap(new HashMap<>(newSettings));
        return of(this, updatedSettings, updatedSettings.inputType != null ? updatedSettings.inputType : this.inputType);
    }
}
