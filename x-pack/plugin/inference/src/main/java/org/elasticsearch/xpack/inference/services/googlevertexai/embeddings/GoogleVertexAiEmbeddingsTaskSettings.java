/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

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
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService.VALID_INPUT_TYPE_VALUES;

public class GoogleVertexAiEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "google_vertex_ai_embeddings_task_settings";

    public static final String AUTO_TRUNCATE = "auto_truncate";

    public static final String INPUT_TYPE = "input_type";

    public static final GoogleVertexAiEmbeddingsTaskSettings EMPTY_SETTINGS = new GoogleVertexAiEmbeddingsTaskSettings(null, null);

    public static GoogleVertexAiEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
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

        Boolean autoTruncate = extractOptionalBoolean(map, AUTO_TRUNCATE, validationException);
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, inputType);
    }

    public static GoogleVertexAiEmbeddingsTaskSettings of(
        GoogleVertexAiEmbeddingsTaskSettings originalSettings,
        GoogleVertexAiEmbeddingsRequestTaskSettings requestSettings
    ) {
        var inputTypeToUse = getValidInputType(originalSettings, requestSettings);
        var autoTruncate = requestSettings.autoTruncate() == null ? originalSettings.autoTruncate : requestSettings.autoTruncate();
        return new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, inputTypeToUse);
    }

    private static InputType getValidInputType(
        GoogleVertexAiEmbeddingsTaskSettings originalSettings,
        GoogleVertexAiEmbeddingsRequestTaskSettings requestTaskSettings
    ) {
        InputType inputTypeToUse = originalSettings.inputType;

        if (requestTaskSettings.inputType() != null) {
            inputTypeToUse = requestTaskSettings.inputType();
        }

        return inputTypeToUse;
    }

    private final InputType inputType;
    private final Boolean autoTruncate;

    public GoogleVertexAiEmbeddingsTaskSettings(@Nullable Boolean autoTruncate, @Nullable InputType inputType) {
        validateInputType(inputType);
        this.inputType = inputType;
        this.autoTruncate = autoTruncate;
    }

    public GoogleVertexAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this.autoTruncate = in.readOptionalBoolean();

        var inputType = (in.getTransportVersion().onOrAfter(TransportVersions.V_8_17_0)) ? in.readOptionalEnum(InputType.class) : null;

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
        return inputType == null && autoTruncate == null;
    }

    public Boolean autoTruncate() {
        return autoTruncate;
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
        return TransportVersions.V_8_15_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(this.autoTruncate);

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_17_0)) {
            out.writeOptionalEnum(this.inputType);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inputType != null) {
            builder.field(INPUT_TYPE, inputType);
        }

        if (autoTruncate != null) {
            builder.field(AUTO_TRUNCATE, autoTruncate);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        GoogleVertexAiEmbeddingsTaskSettings that = (GoogleVertexAiEmbeddingsTaskSettings) object;
        return Objects.equals(inputType, that.inputType) && Objects.equals(autoTruncate, that.autoTruncate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(autoTruncate, inputType);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        GoogleVertexAiEmbeddingsRequestTaskSettings updatedSettings = GoogleVertexAiEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(newSettings)
        );
        return of(this, updatedSettings);
    }
}
