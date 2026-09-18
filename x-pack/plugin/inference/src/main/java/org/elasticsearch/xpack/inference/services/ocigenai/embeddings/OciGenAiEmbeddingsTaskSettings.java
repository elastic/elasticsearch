/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiUtils;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.SettingsScope.TASK_SETTINGS;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.INPUT_TYPE;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.TRUNCATE;

/**
 * Task settings of the OCI Generative AI text embedding task: the {@code input_type} the texts are embedded for and the
 * {@code truncate} strategy applied to inputs that exceed the model's context length.
 *
 * @see <a href="https://docs.oracle.com/en-us/iaas/api/#/en/generative-ai-inference/20231130/datatypes/EmbedTextDetails">EmbedTextDetails</a>
 */
public class OciGenAiEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "oci_genai_embeddings_task_settings";
    public static final OciGenAiEmbeddingsTaskSettings EMPTY_SETTINGS = new OciGenAiEmbeddingsTaskSettings(null, null);

    /** The input types a user may specify in the task settings or the request. */
    public static final EnumSet<InputType> VALID_REQUEST_INPUT_TYPES = EnumSet.of(
        InputType.INGEST,
        InputType.SEARCH,
        InputType.CLASSIFICATION,
        InputType.CLUSTERING
    );

    public static OciGenAiEmbeddingsTaskSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        var validationException = new ValidationException();
        var inputType = extractOptionalEnum(
            map,
            INPUT_TYPE,
            TASK_SETTINGS,
            InputType::fromString,
            VALID_REQUEST_INPUT_TYPES,
            validationException
        );
        var truncation = extractOptionalEnum(map, TRUNCATE, TASK_SETTINGS, Truncation::fromString, Truncation.ALL, validationException);
        validationException.throwIfValidationErrorsExist();

        return new OciGenAiEmbeddingsTaskSettings(inputType, truncation);
    }

    /**
     * Creates a new {@link OciGenAiEmbeddingsTaskSettings} preferring the non-null fields of the request settings over the original
     * settings.
     */
    public static OciGenAiEmbeddingsTaskSettings of(
        OciGenAiEmbeddingsTaskSettings originalSettings,
        OciGenAiEmbeddingsTaskSettings requestTaskSettings
    ) {
        return new OciGenAiEmbeddingsTaskSettings(
            requestTaskSettings.inputType != null ? requestTaskSettings.inputType : originalSettings.inputType,
            requestTaskSettings.truncation != null ? requestTaskSettings.truncation : originalSettings.truncation
        );
    }

    private final InputType inputType;
    private final Truncation truncation;

    public OciGenAiEmbeddingsTaskSettings(@Nullable InputType inputType, @Nullable Truncation truncation) {
        this.inputType = inputType;
        this.truncation = truncation;
    }

    public OciGenAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalEnum(InputType.class), in.readOptionalEnum(Truncation.class));
    }

    @Nullable
    public InputType getInputType() {
        return inputType;
    }

    @Nullable
    public Truncation getTruncation() {
        return truncation;
    }

    @Override
    public boolean isEmpty() {
        return inputType == null && truncation == null;
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return of(this, fromMap(newSettings));
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

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return OciGenAiUtils.ML_INFERENCE_OCI_GENAI_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(OciGenAiUtils.ML_INFERENCE_OCI_GENAI_ADDED);
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
        OciGenAiEmbeddingsTaskSettings that = (OciGenAiEmbeddingsTaskSettings) o;
        return inputType == that.inputType && truncation == that.truncation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputType, truncation);
    }
}
