/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;

/**
 * Defines the task settings for internal rerank service.
 */
public class CustomElandRerankTaskSettings implements TaskSettings {

    public static final String NAME = "custom_eland_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";

    static final CustomElandRerankTaskSettings DEFAULT_SETTINGS = new CustomElandRerankTaskSettings(Boolean.TRUE);

    public static CustomElandRerankTaskSettings defaultsFromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return DEFAULT_SETTINGS;
        }

        Boolean returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, validationException);
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        if (returnDocuments == null) {
            returnDocuments = true;
        }

        return new CustomElandRerankTaskSettings(returnDocuments);
    }

    /**
     * From map without any validation
     * @param map source map
     * @return Task settings
     */
    public static CustomElandRerankTaskSettings fromMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return DEFAULT_SETTINGS;
        }

        Boolean returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, new ValidationException());
        return new CustomElandRerankTaskSettings(returnDocuments);
    }

    /**
     * Return either the request or original settings by preferring non-null fields
     * from the request settings over the original settings.
     *
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return Either {@code originalSettings} or {@code requestTaskSettings}
     */
    public static CustomElandRerankTaskSettings of(
        CustomElandRerankTaskSettings originalSettings,
        CustomElandRerankTaskSettings requestTaskSettings
    ) {
        return requestTaskSettings.returnDocuments() != null ? requestTaskSettings : originalSettings;
    }

    private final Boolean returnDocuments;

    public CustomElandRerankTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalBoolean());
    }

    public CustomElandRerankTaskSettings(@Nullable Boolean doReturnDocuments) {
        if (doReturnDocuments == null) {
            this.returnDocuments = true;
        } else {
            this.returnDocuments = doReturnDocuments;
        }
    }

    @Override
    public boolean isEmpty() {
        return returnDocuments == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS, returnDocuments);
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
        return TransportVersions.V_8_14_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(returnDocuments);
    }

    public Boolean returnDocuments() {
        return returnDocuments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomElandRerankTaskSettings that = (CustomElandRerankTaskSettings) o;
        return Objects.equals(returnDocuments, that.returnDocuments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnDocuments);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        CustomElandRerankTaskSettings updatedSettings = CustomElandRerankTaskSettings.fromMap(new HashMap<>(newSettings));
        return of(this, updatedSettings);
    }
}
