/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleDiscoveryEngineRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.LOCATION;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.PROJECT_ID;

public class GoogleVertexAiChatCompletionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        GoogleDiscoveryEngineRateLimitServiceSettings {

    public static final String NAME = "google_vertex_ai_chatcompletion_service_settings";

    private final String location;
    private final String modelId;
    private final String projectId;

    private final RateLimitSettings rateLimitSettings;

    // https://cloud.google.com/vertex-ai/docs/quotas#eval-quotas
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1000);

    public GoogleVertexAiChatCompletionServiceSettings(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readString(), new RateLimitSettings(in));
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(PROJECT_ID, projectId);
        builder.field(LOCATION, location);
        builder.field(MODEL_ID, modelId);
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    public static GoogleVertexAiChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        // Extract required fields
        String projectId = ServiceUtils.extractRequiredString(map, PROJECT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String location = ServiceUtils.extractRequiredString(map, LOCATION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String modelId = ServiceUtils.extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        // Extract rate limit settings
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

        return new GoogleVertexAiChatCompletionServiceSettings(projectId, location, modelId, rateLimitSettings);
    }

    public GoogleVertexAiChatCompletionServiceSettings(
        String projectId,
        String location,
        String modelId,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.projectId = projectId;
        this.location = location;
        this.modelId = modelId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public String location() {
        return location;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public String projectId() {
        return projectId;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_VERTEXAI_CHATCOMPLETION_ADDED_8_19;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(projectId);
        out.writeString(location);
        out.writeString(modelId);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GoogleVertexAiChatCompletionServiceSettings that = (GoogleVertexAiChatCompletionServiceSettings) o;
        return Objects.equals(location, that.location)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(projectId, that.projectId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, modelId, projectId, rateLimitSettings);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }
}
