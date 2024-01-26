/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

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
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields.TRUNCATE;

/**
 * Defines the task settings for the cohere text embeddings service.
 *
 * <p>
 * <a href="https://docs.cohere.com/reference/embed">See api docs for details.</a>
 * </p>
 */
public class CohereEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "cohere_embeddings_task_settings";
    public static final CohereEmbeddingsTaskSettings EMPTY_SETTINGS = new CohereEmbeddingsTaskSettings(null, null);
    static final String INPUT_TYPE = "input_type";
    private static final InputType[] VALID_REQUEST_VALUES = { InputType.INGEST, InputType.SEARCH };
    private static final List<InputType> VALID_INPUT_VALUES_LIST = Arrays.asList(VALID_REQUEST_VALUES);

    public static CohereEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
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
        CohereTruncation truncation = extractOptionalEnum(
            map,
            TRUNCATE,
            ModelConfigurations.TASK_SETTINGS,
            CohereTruncation::fromString,
            CohereTruncation.values(),
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CohereEmbeddingsTaskSettings(inputType, truncation);
    }

    private final InputType inputType;
    private final CohereTruncation truncation;

    public CohereEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalEnum(InputType.class), in.readOptionalEnum(CohereTruncation.class));
    }

    public CohereEmbeddingsTaskSettings(@Nullable InputType inputType, @Nullable CohereTruncation truncation) {
        validateInputType(inputType);
        this.inputType = inputType;
        this.truncation = truncation;
    }

    private static void validateInputType(InputType inputType) {
        if (inputType == null) {
            return;
        }

        assert VALID_INPUT_VALUES_LIST.contains(inputType) : invalidInputTypeMessage(inputType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inputType != null) {
            builder.field(INPUT_TYPE, inputType);
        }

        if (truncation != null) {
            builder.field(TRUNCATE, truncation);
        }
        builder.endObject();
        return builder;
    }

    public InputType getInputType() {
        return inputType;
    }

    public CohereTruncation getTruncation() {
        return truncation;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_COHERE_EMBEDDINGS_ADDED;
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
        CohereEmbeddingsTaskSettings that = (CohereEmbeddingsTaskSettings) o;
        return inputType == that.inputType && truncation == that.truncation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputType, truncation);
    }

    public CohereEmbeddingsTaskSettings overrideWith(CohereEmbeddingsTaskSettings requestTaskSettings) {
        if (requestTaskSettings.equals(EMPTY_SETTINGS)) {
            return this;
        }

        var inputTypeToUse = requestTaskSettings.getInputType() == null ? inputType : requestTaskSettings.getInputType();
        var truncationToUse = requestTaskSettings.getTruncation() == null ? truncation : requestTaskSettings.getTruncation();

        return new CohereEmbeddingsTaskSettings(inputTypeToUse, truncationToUse);
    }

    /**
     * Sets the input type field for the task settings if input type value is valid.
     * @param inputType the new input type to use
     * @return newly updated task settings
     */
    public CohereEmbeddingsTaskSettings setInputType(InputType inputType) {
        if (VALID_INPUT_VALUES_LIST.contains(inputType) == false) {
            return this;
        }

        return new CohereEmbeddingsTaskSettings(inputType, truncation);
    }

    public static String invalidInputTypeMessage(InputType inputType) {
        return Strings.format("received invalid input type value [%s]", inputType.toString());
    }
}
