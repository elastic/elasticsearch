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
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;

public class GoogleVertexAiEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "google_vertex_ai_embeddings_task_settings";

    public static final String AUTO_TRUNCATE = "auto_truncate";

    public static final GoogleVertexAiEmbeddingsTaskSettings EMPTY_SETTINGS = new GoogleVertexAiEmbeddingsTaskSettings(
        Boolean.valueOf(null)
    );

    public static GoogleVertexAiEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        Boolean autoTruncate = extractOptionalBoolean(map, AUTO_TRUNCATE, validationException);
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate);
    }

    public static GoogleVertexAiEmbeddingsTaskSettings of(
        GoogleVertexAiEmbeddingsTaskSettings originalSettings,
        GoogleVertexAiEmbeddingsRequestTaskSettings requestSettings
    ) {
        var autoTruncate = requestSettings.autoTruncate() == null ? originalSettings.autoTruncate : requestSettings.autoTruncate();
        return new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate);
    }

    private final Boolean autoTruncate;

    public GoogleVertexAiEmbeddingsTaskSettings(@Nullable Boolean autoTruncate) {
        this.autoTruncate = autoTruncate;
    }

    public GoogleVertexAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this.autoTruncate = in.readOptionalBoolean();
    }

    @Override
    public boolean isEmpty() {
        return autoTruncate == null;
    }

    public Boolean autoTruncate() {
        return autoTruncate;
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
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
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
        return Objects.equals(autoTruncate, that.autoTruncate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(autoTruncate);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        GoogleVertexAiEmbeddingsRequestTaskSettings requestSettings = GoogleVertexAiEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(newSettings)
        );
        return of(this, requestSettings);
    }
}
