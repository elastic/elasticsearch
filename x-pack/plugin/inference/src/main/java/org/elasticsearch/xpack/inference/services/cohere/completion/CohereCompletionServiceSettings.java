/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.cohere.CohereRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereService;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings.API_VERSION;
import static org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings.MODEL_REQUIRED_FOR_V2_API;
import static org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings.apiVersionFromMap;

/**
 * Settings for the Cohere completion service.
 * This class encapsulates the configuration settings required to use Cohere models for generating completions.
 */
public class CohereCompletionServiceSettings extends FilteredXContentObject implements ServiceSettings, CohereRateLimitServiceSettings {

    public static final String NAME = "cohere_completion_service_settings";
    private static final TransportVersion ML_INFERENCE_COHERE_API_VERSION = TransportVersion.fromName("ml_inference_cohere_api_version");

    // Production key rate limits for all endpoints: https://docs.cohere.com/docs/going-live#production-key-specifications
    // 10K requests per minute
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    /**
     * Creates an instance of {@link CohereCompletionServiceSettings} from a map of settings.
     *
     * @param map The map containing the settings.
     * @param context The context for configuration parsing.
     * @return the created {@link CohereCompletionServiceSettings}.
     * @throws ValidationException If there are validation errors in the provided settings.
     */
    public static CohereCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var uri = extractOptionalUri(map, URL, validationException);
        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, CohereService.NAME, context);
        var modelId = extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var apiVersion = apiVersionFromMap(map, context, validationException);
        if (apiVersion == CohereServiceSettings.CohereApiVersion.V2 && modelId == null) {
            validationException.addValidationError(MODEL_REQUIRED_FOR_V2_API);
        }

        validationException.throwIfValidationErrorsExist();

        return new CohereCompletionServiceSettings(uri, modelId, rateLimitSettings, apiVersion);
    }

    private final URI uri;
    private final String modelId;
    private final RateLimitSettings rateLimitSettings;
    private final CohereServiceSettings.CohereApiVersion apiVersion;

    public CohereCompletionServiceSettings(
        @Nullable URI uri,
        @Nullable String modelId,
        @Nullable RateLimitSettings rateLimitSettings,
        CohereServiceSettings.CohereApiVersion apiVersion
    ) {
        this.uri = uri;
        this.modelId = modelId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
        this.apiVersion = apiVersion;
    }

    public CohereCompletionServiceSettings(
        @Nullable String url,
        @Nullable String modelId,
        @Nullable RateLimitSettings rateLimitSettings,
        CohereServiceSettings.CohereApiVersion apiVersion
    ) {
        this(createOptionalUri(url), modelId, rateLimitSettings, apiVersion);
    }

    /**
     * Creates {@link CohereCompletionServiceSettings} from a {@link StreamInput}.
     * @param in the stream input
     * @throws IOException if an I/O exception occurs
     */
    public CohereCompletionServiceSettings(StreamInput in) throws IOException {
        uri = createOptionalUri(in.readOptionalString());
        modelId = in.readOptionalString();
        rateLimitSettings = new RateLimitSettings(in);
        if (in.getTransportVersion().supports(ML_INFERENCE_COHERE_API_VERSION)) {
            this.apiVersion = in.readEnum(CohereServiceSettings.CohereApiVersion.class);
        } else {
            this.apiVersion = CohereServiceSettings.CohereApiVersion.V1;
        }
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public CohereServiceSettings.CohereApiVersion apiVersion() {
        return apiVersion;
    }

    public URI uri() {
        return uri;
    }

    public String modelId() {
        return modelId;
    }

    @Override
    public CohereCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings, TaskType taskType) {
        var validationException = new ValidationException();

        var extractedUri = extractOptionalUri(serviceSettings, URL, validationException);
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            CohereService.NAME,
            ConfigurationParseContext.REQUEST
        );
        var extractedModelId = extractOptionalString(serviceSettings, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var extractedApiVersion = ServiceUtils.extractOptionalEnum(
            serviceSettings,
            API_VERSION,
            ModelConfigurations.SERVICE_SETTINGS,
            CohereServiceSettings.CohereApiVersion::fromString,
            EnumSet.allOf(CohereServiceSettings.CohereApiVersion.class),
            validationException
        );
        if (extractedApiVersion == CohereServiceSettings.CohereApiVersion.V2 && extractedModelId == null && this.modelId == null) {
            validationException.addValidationError(MODEL_REQUIRED_FOR_V2_API);
        }

        validationException.throwIfValidationErrorsExist();

        return new CohereCompletionServiceSettings(
            extractedUri != null ? extractedUri : this.uri,
            extractedModelId != null ? extractedModelId : this.modelId,
            extractedRateLimitSettings,
            extractedApiVersion != null ? extractedApiVersion : this.apiVersion
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);
        builder.field(API_VERSION, apiVersion); // API version is persisted but not exposed to the user

        builder.endObject();
        return builder;
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
        var uriToWrite = uri != null ? uri.toString() : null;
        out.writeOptionalString(uriToWrite);
        out.writeOptionalString(modelId);
        rateLimitSettings.writeTo(out);
        if (out.getTransportVersion().supports(ML_INFERENCE_COHERE_API_VERSION)) {
            out.writeEnum(apiVersion);
        }
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        if (uri != null) {
            builder.field(URL, uri.toString());
        }

        if (modelId != null) {
            builder.field(MODEL_ID, modelId);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        CohereCompletionServiceSettings that = (CohereCompletionServiceSettings) object;
        return Objects.equals(uri, that.uri)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && apiVersion == that.apiVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, modelId, rateLimitSettings, apiVersion);
    }
}
