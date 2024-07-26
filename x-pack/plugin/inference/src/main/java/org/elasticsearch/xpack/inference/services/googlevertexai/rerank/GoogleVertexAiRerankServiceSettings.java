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
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.PROJECT_ID;

public class GoogleVertexAiRerankServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        GoogleDiscoveryEngineRateLimitServiceSettings {

    public static final String NAME = "google_vertex_ai_rerank_service_settings";

    // Query requests per project per minute: https://cloud.google.com/generative-ai-app-builder/quotas#request_quotas
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(300);

    public static GoogleVertexAiRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String projectId = extractRequiredString(map, PROJECT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String model = extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            GoogleVertexAiService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GoogleVertexAiRerankServiceSettings(projectId, model, rateLimitSettings);
    }

    private final String projectId;

    private final String modelId;

    private final RateLimitSettings rateLimitSettings;

    public GoogleVertexAiRerankServiceSettings(String projectId, @Nullable String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        this.projectId = Objects.requireNonNull(projectId);
        this.modelId = modelId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public GoogleVertexAiRerankServiceSettings(StreamInput in) throws IOException {
        this.projectId = in.readString();
        this.modelId = in.readOptionalString();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    public String projectId() {
        return projectId;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_GOOGLE_VERTEX_AI_RERANKING_ADDED;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(PROJECT_ID, projectId);

        if (modelId != null) {
            builder.field(MODEL_ID, modelId);
        }

        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);

        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(projectId);
        out.writeOptionalString(modelId);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        GoogleVertexAiRerankServiceSettings that = (GoogleVertexAiRerankServiceSettings) object;
        return Objects.equals(projectId, that.projectId)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectId, modelId, rateLimitSettings);
    }
}
