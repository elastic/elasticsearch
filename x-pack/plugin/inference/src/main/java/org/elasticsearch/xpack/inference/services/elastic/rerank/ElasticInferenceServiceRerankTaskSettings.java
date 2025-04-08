/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

public class ElasticInferenceServiceRerankTaskSettings implements TaskSettings {

    public static final String NAME = "elastic_rerank_task_settings";
    public static final String TOP_N_DOCS_ONLY = "top_n";

    public static final ElasticInferenceServiceRerankTaskSettings EMPTY_SETTINGS = new ElasticInferenceServiceRerankTaskSettings(
        (Integer) null
    );

    public static ElasticInferenceServiceRerankTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        Integer topNDocumentsOnly = extractOptionalPositiveInteger(
            map,
            TOP_N_DOCS_ONLY,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new ElasticInferenceServiceRerankTaskSettings(topNDocumentsOnly);
    }

    /**
     * Creates a new {@link ElasticInferenceServiceRerankTaskSettings} by preferring non-null fields from the request settings
     * over the original settings.
     *
     * @param originalSettings      the settings stored as part of the inference entity configuration
     * @param requestTaskSettings   the settings passed within the task_settings field of the request
     * @return new instance of {@link ElasticInferenceServiceRerankTaskSettings}
     */
    public static ElasticInferenceServiceRerankTaskSettings of(
        ElasticInferenceServiceRerankTaskSettings originalSettings,
        ElasticInferenceServiceRerankTaskSettings requestTaskSettings
    ) {
        return new ElasticInferenceServiceRerankTaskSettings(
            Objects.requireNonNullElse(requestTaskSettings.getTopNDocumentsOnly(), originalSettings.getTopNDocumentsOnly())
        );
    }

    private final Integer topNDocumentsOnly;

    public ElasticInferenceServiceRerankTaskSettings(StreamInput in) throws IOException {
        this.topNDocumentsOnly = in.readOptionalVInt();
    }

    public ElasticInferenceServiceRerankTaskSettings(@Nullable Integer topNDocumentsOnly) {
        this.topNDocumentsOnly = topNDocumentsOnly;
    }

    public Integer getTopNDocumentsOnly() {
        return topNDocumentsOnly;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_ELASTIC_RERANK;
    }

    @Override
    public boolean isEmpty() {
        return topNDocumentsOnly == null;
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        ElasticInferenceServiceRerankTaskSettings updatedSettings = ElasticInferenceServiceRerankTaskSettings.fromMap(
            new HashMap<>(newSettings)
        );
        return ElasticInferenceServiceRerankTaskSettings.of(this, updatedSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (Objects.nonNull(topNDocumentsOnly)) {
            builder.field(TOP_N_DOCS_ONLY, topNDocumentsOnly);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(topNDocumentsOnly);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticInferenceServiceRerankTaskSettings that = (ElasticInferenceServiceRerankTaskSettings) o;
        return Objects.equals(topNDocumentsOnly, that.topNDocumentsOnly);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topNDocumentsOnly);
    }
}
