/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

/**
 * Common service settings shared by all TencentCloud task types.
 * Contains the required {@code model_id}, an optional {@code url} override, and rate limit settings.
 */
public class TencentCloudCommonServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        TencentCloudRateLimitServiceSettings {

    public static final String NAME = "tencentcloud_service_settings";

    // Default rate limit for TencentCloud AI Gateway (see docs).
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(20);

    @Nullable
    public static TencentCloudCommonServiceSettings fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();

        var modelId = extractRequiredString(map, ServiceFields.MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = extractOptionalUri(map, ServiceFields.URL, validationException);
        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, context);

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }
        return new TencentCloudCommonServiceSettings(modelId, uri, rateLimitSettings);
    }

    private final String modelId;
    @Nullable
    private final URI uri;
    private final RateLimitSettings rateLimitSettings;

    public TencentCloudCommonServiceSettings(String modelId, @Nullable URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.uri = uri;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public TencentCloudCommonServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        var uriString = in.readOptionalString();
        this.uri = uriString == null ? null : URI.create(uriString);
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Nullable
    public TencentCloudCommonServiceSettings updateCommonServiceSettings(
        Map<String, Object> serviceSettings,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();

        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            ConfigurationParseContext.REQUEST
        );
        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        return new TencentCloudCommonServiceSettings(this.modelId, this.uri, extractedRateLimitSettings);
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Nullable
    public URI uri() {
        return uri;
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        return toXContentFragmentOfExposedFields(builder, params);
    }

    @Override
    public XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(ServiceFields.MODEL_ID, modelId);
        if (uri != null) {
            builder.field(ServiceFields.URL, uri.toString());
        }
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalString(uri != null ? uri.toString() : null);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TencentCloudCommonServiceSettings that = (TencentCloudCommonServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, rateLimitSettings);
    }
}
