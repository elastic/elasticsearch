/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.SettingsScope;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Task settings for the Elastic Inference Service {@code document_extraction} task type. They can be stored on the inference endpoint
 * and overridden per request through the {@code task_settings} field of the document extraction request body, where the request
 * value wins (see {@link #of(ElasticInferenceServiceDocumentExtractionTaskSettings, ElasticInferenceServiceDocumentExtractionTaskSettings)}).
 */
public class ElasticInferenceServiceDocumentExtractionTaskSettings implements TaskSettings {

    public static final String NAME = "elastic_inference_service_document_extraction_task_settings";
    public static final String OUTPUT_FORMAT = "output_format";

    private static final TransportVersion INFERENCE_API_EIS_DOCUMENT_EXTRACTION_ADDED = TransportVersion.fromName(
        "inference_api_eis_document_extraction_added"
    );

    public static final ElasticInferenceServiceDocumentExtractionTaskSettings EMPTY_SETTINGS =
        new ElasticInferenceServiceDocumentExtractionTaskSettings((String) null);

    /**
     * Parses task settings from a raw config map, removing the fields it recognizes so callers can reject leftover unknown fields.
     * A null or empty map produces {@link #EMPTY_SETTINGS}.
     */
    public static ElasticInferenceServiceDocumentExtractionTaskSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        String outputFormat = ServiceUtils.extractOptionalString(map, OUTPUT_FORMAT, SettingsScope.TASK_SETTINGS, validationException);

        validationException.throwIfValidationErrorsExist();

        return new ElasticInferenceServiceDocumentExtractionTaskSettings(outputFormat);
    }

    /**
     * Merges stored and request task settings: a field set in {@code requestSettings} overrides the stored value, otherwise the stored
     * value is kept.
     */
    public static ElasticInferenceServiceDocumentExtractionTaskSettings of(
        ElasticInferenceServiceDocumentExtractionTaskSettings originalSettings,
        ElasticInferenceServiceDocumentExtractionTaskSettings requestSettings
    ) {
        return new ElasticInferenceServiceDocumentExtractionTaskSettings(
            requestSettings.outputFormat != null ? requestSettings.outputFormat : originalSettings.outputFormat
        );
    }

    private final String outputFormat;

    public ElasticInferenceServiceDocumentExtractionTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalString());
    }

    public ElasticInferenceServiceDocumentExtractionTaskSettings(@Nullable String outputFormat) {
        this.outputFormat = outputFormat;
    }

    @Nullable
    public String outputFormat() {
        return outputFormat;
    }

    @Override
    public boolean isEmpty() {
        return outputFormat == null || outputFormat.isEmpty();
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return of(this, fromMap(newSettings));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return INFERENCE_API_EIS_DOCUMENT_EXTRACTION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(outputFormat);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (outputFormat != null && outputFormat.isEmpty() == false) {
            builder.field(OUTPUT_FORMAT, outputFormat);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticInferenceServiceDocumentExtractionTaskSettings that = (ElasticInferenceServiceDocumentExtractionTaskSettings) o;
        return Objects.equals(outputFormat, that.outputFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(outputFormat);
    }
}
