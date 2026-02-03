/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
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
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUtils;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.LOCATION;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.PROJECT_ID;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.STREAMING_URL_SETTING_NAME;

/**
 * Settings for the Google Vertex AI chat completion service.
 * This class contains the settings required to configure a Google Vertex AI chat completion service.
 */
public class GoogleVertexAiChatCompletionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        GoogleVertexAiRateLimitServiceSettings {

    public static final String NAME = "google_vertex_ai_chatcompletion_service_settings";

    private static final TransportVersion ML_INFERENCE_VERTEXAI_CHATCOMPLETION_ADDED = TransportVersion.fromName(
        "ml_inference_vertexai_chatcompletion_added"
    );

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
        var version = in.getTransportVersion();
        String projectIdFromStreamInput;
        String locationFromStreamInput;
        String modelIdFromStreamInput;
        URI uriFromStreamInput = null;
        URI streamingUriFromStreamInput = null;
        GoogleModelGardenProvider providerFromStreamInput = null;

        if (GoogleVertexAiUtils.supportsModelGarden(version)) {
            projectIdFromStreamInput = in.readOptionalString();
            locationFromStreamInput = in.readOptionalString();
            modelIdFromStreamInput = in.readOptionalString();
            uriFromStreamInput = ServiceUtils.createOptionalUri(in.readOptionalString());
            streamingUriFromStreamInput = ServiceUtils.createOptionalUri(in.readOptionalString());
            providerFromStreamInput = in.readOptionalEnum(GoogleModelGardenProvider.class);
        } else {
            projectIdFromStreamInput = in.readString();
            locationFromStreamInput = in.readString();
            modelIdFromStreamInput = in.readString();
        }
        RateLimitSettings rateLimitSettingsFromStreamInput = new RateLimitSettings(in);

        this.projectId = Strings.isNullOrEmpty(projectIdFromStreamInput) ? null : projectIdFromStreamInput;
        this.location = Strings.isNullOrEmpty(locationFromStreamInput) ? null : locationFromStreamInput;
        this.modelId = Strings.isNullOrEmpty(modelIdFromStreamInput) ? null : modelIdFromStreamInput;
        this.uri = uriFromStreamInput;
        this.streamingUri = streamingUriFromStreamInput;
        // Default to GOOGLE if not set
        this.provider = Objects.requireNonNullElse(providerFromStreamInput, GoogleModelGardenProvider.GOOGLE);
        this.rateLimitSettings = rateLimitSettingsFromStreamInput;

    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (Strings.isNullOrEmpty(projectId) == false) {
            builder.field(PROJECT_ID, projectId);
        }
        if (Strings.isNullOrEmpty(location) == false) {
            builder.field(LOCATION, location);
        }
        if (Strings.isNullOrEmpty(modelId) == false) {
            builder.field(MODEL_ID, modelId);
        }
        if (uri != null) {
            builder.field(URL, uri.toString());
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
        URI uri = ServiceUtils.extractOptionalUri(map, URL, validationException);
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

        validateServiceSettings(provider, uri, streamingUri, projectId, location, modelId, validationException);

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

    private static void validateServiceSettings(
        GoogleModelGardenProvider provider,
        URI uri,
        URI streamingUri,
        String projectId,
        String location,
        String modelId,
        ValidationException validationException
    ) {
        // GOOGLE is the default provider, so if provider is null, we treat it as GOOGLE
        boolean isNonGoogleProvider = provider != null && provider != GoogleModelGardenProvider.GOOGLE;
        // If using a non-Google provider, at least one URL must be provided
        boolean hasAnyUrl = uri != null || streamingUri != null;
        // If using Google Vertex AI, all three fields must be provided
        boolean hasAllVertexFields = projectId != null && location != null && modelId != null;

        if (isNonGoogleProvider) {
            if (hasAnyUrl == false) {
                // Non-Google (Model Garden endpoint mode): must have at least one URL. Google Vertex AI fields are allowed.
                validationException.addValidationError(
                    String.format(
                        Locale.ROOT,
                        "Google Model Garden provider=%s selected. Either 'uri' or 'streaming_uri' must be provided",
                        provider
                    )
                );
            }
        } else if (hasAnyUrl) {
            // If using Google Vertex AI, URLs must not be provided
            validationException.addValidationError(String.format(Locale.ROOT, """
                'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must not be provided. \
                Remove 'url' and 'streaming_url' fields. Provided values: uri=%s, streaming_uri=%s""", uri, streamingUri));
        } else if (hasAllVertexFields == false) {
            // If using Google Vertex AI, all fields must be provided
            validationException.addValidationError(String.format(Locale.ROOT, """
                For Google Vertex AI models, you must provide 'location', 'project_id', and 'model_id'. \
                Provided values: location=%s, project_id=%s, model_id=%s""", location, projectId, modelId));
        }
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
        this.provider = Objects.requireNonNullElse(provider, GoogleModelGardenProvider.GOOGLE);
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
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return ML_INFERENCE_VERTEXAI_CHATCOMPLETION_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(ML_INFERENCE_VERTEXAI_CHATCOMPLETION_ADDED);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportVersion version = out.getTransportVersion();

        if (GoogleVertexAiUtils.supportsModelGarden(version)) {
            out.writeOptionalString(projectId);
            out.writeOptionalString(location);
            out.writeOptionalString(modelId);
            out.writeOptionalString(uri != null ? uri.toString() : null);
            out.writeOptionalString(streamingUri != null ? streamingUri.toString() : null);
            out.writeOptionalEnum(provider);
        } else {
            out.writeString(Objects.requireNonNullElse(projectId, ""));
            out.writeString(Objects.requireNonNullElse(location, ""));
            out.writeString(Objects.requireNonNullElse(modelId, ""));
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
    public String toString() {
        return Strings.toString(this);
    }
}
