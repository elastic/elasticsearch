/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.rerank;

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

public class GoogleVertexAiRerankTaskSettings implements TaskSettings {

    public static final String NAME = "google_vertex_ai_rerank_task_settings";

    public static final String TOP_N = "top_n";

    public static GoogleVertexAiRerankTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        Integer topN = extractOptionalPositiveInteger(map, TOP_N, ModelConfigurations.TASK_SETTINGS, validationException);
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GoogleVertexAiRerankTaskSettings(topN);
    }

    public static GoogleVertexAiRerankTaskSettings of(
        GoogleVertexAiRerankTaskSettings originalSettings,
        GoogleVertexAiRerankRequestTaskSettings requestSettings
    ) {
        var topN = requestSettings.topN() == null ? originalSettings.topN() : requestSettings.topN();
        return new GoogleVertexAiRerankTaskSettings(topN);
    }

    private final Integer topN;

    public GoogleVertexAiRerankTaskSettings(@Nullable Integer topN) {
        this.topN = topN;
    }

    public GoogleVertexAiRerankTaskSettings(StreamInput in) throws IOException {
        this.topN = in.readOptionalVInt();
    }

    @Override
    public boolean isEmpty() {
        return topN == null;
    }

    public Integer topN() {
        return topN;
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
        out.writeOptionalVInt(topN);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (topN != null) {
            builder.field(TOP_N, topN);
        }

        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        GoogleVertexAiRerankTaskSettings that = (GoogleVertexAiRerankTaskSettings) object;
        return Objects.equals(topN, that.topN);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topN);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        GoogleVertexAiRerankRequestTaskSettings requestSettings = GoogleVertexAiRerankRequestTaskSettings.fromMap(
            new HashMap<>(newSettings)
        );
        return of(this, requestSettings);
    }
}
