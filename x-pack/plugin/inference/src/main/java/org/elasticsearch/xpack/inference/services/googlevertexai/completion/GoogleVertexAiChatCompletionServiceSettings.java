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
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleModelGardenProvider;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleDiscoveryEngineRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.LOCATION;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.PROJECT_ID;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.STREAMING_URL_SETTING_NAME;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.URL_SETTING_NAME;

/**
 * Settings for the Google Vertex AI chat completion service.
 * This class contains the settings required to configure a Google Vertex AI chat completion service.
 */
public class GoogleVertexAiChatCompletionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        GoogleDiscoveryEngineRateLimitServiceSettings {

    public static final String NAME = "google_vertex_ai_chatcompletion_service_settings";

    private final String location;
    private final String modelId;
    private final String projectId;

    private final URI uri;
    private final URI streamingUri;
    private final GoogleModelGardenProvider provider;

    private final RateLimitSettings rateLimitSettings;

    // https://cloud.google.com/vertex-ai/docs/quotas#eval-quotas
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1000);

    public GoogleVertexAiChatCompletionServiceSettings(StreamInput in) throws IOException {
        this(
            in.readOptionalString(),
            in.readOptionalString(),
            in.readOptionalString(),
            ServiceUtils.createOptionalUri(in.readString()),
            ServiceUtils.createOptionalUri(in.readString()),
            in.readOptionalEnum(GoogleModelGardenProvider.class),
            new RateLimitSettings(in)
        );
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (projectId != null) {
            builder.field(PROJECT_ID, projectId);
        }
        if (location != null) {
            builder.field(LOCATION, location);
        }
        if (modelId != null) {
            builder.field(MODEL_ID, modelId);
        }
        if (uri != null) {
            builder.field(URL_SETTING_NAME, uri.toString());
        }
        if (streamingUri != null) {
            builder.field(STREAMING_URL_SETTING_NAME, streamingUri.toString());
        }
        if (provider != null) {
            builder.field(PROVIDER_SETTING_NAME, provider.name());
        }
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    public static GoogleVertexAiChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        // Extract Google Vertex AI fields
        String projectId = ServiceUtils.extractOptionalString(map, PROJECT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String location = ServiceUtils.extractOptionalString(map, LOCATION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String modelId = ServiceUtils.extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        // Extract Google Model Garden fields
        URI uri = ServiceUtils.extractOptionalUri(map, URL_SETTING_NAME, validationException);
        URI streamingUri = ServiceUtils.extractOptionalUri(map, STREAMING_URL_SETTING_NAME, validationException);
        GoogleModelGardenProvider provider = ServiceUtils.extractOptionalEnum(
            map,
            PROVIDER_SETTING_NAME,
            ModelConfigurations.SERVICE_SETTINGS,
            GoogleModelGardenProvider::fromString,
            EnumSet.allOf(GoogleModelGardenProvider.class),
            validationException
        );

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

        return new GoogleVertexAiChatCompletionServiceSettings(
            projectId,
            location,
            modelId,
            uri,
            streamingUri,
            provider,
            rateLimitSettings
        );
    }

    public GoogleVertexAiChatCompletionServiceSettings(
        @Nullable String projectId,
        @Nullable String location,
        @Nullable String modelId,
        @Nullable URI uri,
        @Nullable URI streamingUri,
        @Nullable GoogleModelGardenProvider provider,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.projectId = projectId;
        this.location = location;
        this.modelId = modelId;
        this.uri = uri;
        this.streamingUri = streamingUri;
        this.provider = provider;
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

    public URI uri() {
        return uri;
    }

    public URI streamingUri() {
        return streamingUri;
    }

    public GoogleModelGardenProvider provider() {
        return provider;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return TransportVersions.ML_INFERENCE_VERTEXAI_CHATCOMPLETION_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.onOrAfter(TransportVersions.ML_INFERENCE_VERTEXAI_CHATCOMPLETION_ADDED)
            || version.isPatchFrom(TransportVersions.ML_INFERENCE_VERTEXAI_CHATCOMPLETION_ADDED_8_19);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(projectId);
        out.writeOptionalString(location);
        out.writeOptionalString(modelId);

        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_GOOGLE_MODEL_GARDEN_ADDED)
            || out.getTransportVersion().isPatchFrom(TransportVersions.ML_INFERENCE_GOOGLE_MODEL_GARDEN_ADDED_8_19)) {
            out.writeOptionalString(uri != null ? uri.toString() : null);
            out.writeOptionalString(streamingUri != null ? streamingUri.toString() : null);
            out.writeOptionalEnum(provider);
        }

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
            && Objects.equals(uri, that.uri)
            && Objects.equals(streamingUri, that.streamingUri)
            && Objects.equals(provider, that.provider)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, modelId, projectId, uri, streamingUri, provider, rateLimitSettings);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }
}
