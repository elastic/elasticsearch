/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.completion;

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
import org.elasticsearch.xpack.inference.services.cohere.CohereRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereService;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings.API_VERSION;
import static org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings.MODEL_REQUIRED_FOR_V2_API;
import static org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings.apiVersionFromMap;

public class CohereCompletionServiceSettings extends FilteredXContentObject implements ServiceSettings, CohereRateLimitServiceSettings {

    public static final String NAME = "cohere_completion_service_settings";

    // Production key rate limits for all endpoints: https://docs.cohere.com/docs/going-live#production-key-specifications
    // 10K requests per minute
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    public static CohereCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        URI uri = convertToUri(url, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            CohereService.NAME,
            context
        );
        String modelId = extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var apiVersion = apiVersionFromMap(map, context, validationException);
        if (apiVersion == CohereServiceSettings.CohereApiVersion.V2) {
            if (modelId == null) {
                validationException.addValidationError(MODEL_REQUIRED_FOR_V2_API);
            }
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

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

    public CohereCompletionServiceSettings(StreamInput in) throws IOException {
        uri = createOptionalUri(in.readOptionalString());
        modelId = in.readOptionalString();
        rateLimitSettings = new RateLimitSettings(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_COHERE_API_VERSION)
            || in.getTransportVersion().isPatchFrom(TransportVersions.ML_INFERENCE_COHERE_API_VERSION)) {
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
        return TransportVersions.V_8_15_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        var uriToWrite = uri != null ? uri.toString() : null;
        out.writeOptionalString(uriToWrite);
        out.writeOptionalString(modelId);
        rateLimitSettings.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_COHERE_API_VERSION)
            || out.getTransportVersion().isPatchFrom(TransportVersions.ML_INFERENCE_COHERE_API_VERSION)) {
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
