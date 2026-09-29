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

public class ElasticInferenceServiceDocumentExtractionTaskSettings implements TaskSettings {

    public static final String NAME = "elastic_inference_service_document_extraction_task_settings";
    public static final String OUTPUT_FORMAT = "output_format";

    static final ElasticInferenceServiceDocumentExtractionTaskSettings EMPTY_SETTINGS =
        new ElasticInferenceServiceDocumentExtractionTaskSettings((String) null);

    public static ElasticInferenceServiceDocumentExtractionTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        String outputFormat = ServiceUtils.extractOptionalString(map, OUTPUT_FORMAT, SettingsScope.TASK_SETTINGS, validationException);

        validationException.throwIfValidationErrorsExist();

        return new ElasticInferenceServiceDocumentExtractionTaskSettings(outputFormat);
    }

    private final String outputFormat;

    public ElasticInferenceServiceDocumentExtractionTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalString());
    }

    public ElasticInferenceServiceDocumentExtractionTaskSettings(@Nullable String outputFormat) {
        this.outputFormat = outputFormat;
    }

    @Override
    public boolean isEmpty() {
        return outputFormat == null || outputFormat.isEmpty();
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        if (newSettings == null || newSettings.isEmpty()) {
            return this;
        }

        return fromMap(newSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
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
}
