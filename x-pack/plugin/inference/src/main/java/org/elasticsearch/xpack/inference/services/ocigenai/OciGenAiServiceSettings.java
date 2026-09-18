/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.SettingsScope.SERVICE_SETTINGS;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.COMPARTMENT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.ENDPOINT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.REGION;

/**
 * Base class for the OCI Generative AI task specific service settings. Holds the fields shared by every task: the region (or an
 * explicit endpoint URL), the compartment OCID, the model id, an optional dedicated AI cluster endpoint OCID and the rate limit.
 */
public abstract class OciGenAiServiceSettings extends FilteredXContentObject implements ServiceSettings {

    /**
     * OCI Generative AI throughput limits for on-demand inference are tenancy and model specific, see
     * <a href="https://docs.oracle.com/en-us/iaas/Content/generative-ai/limits.htm">Generative AI limits</a>. A conservative default
     * is used; override it with {@code rate_limit.requests_per_minute} to match the tenancy's limit for the model.
     */
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1_000);

    private static final Pattern REGION_PATTERN = Pattern.compile("[a-z0-9-]+");

    /**
     * The settings shared by all OCI Generative AI tasks.
     *
     * @param region            the OCI region identifier; may be {@code null} only when {@code uri} is provided
     * @param compartmentId     the compartment OCID
     * @param modelId           the OCI Generative AI model id (for example {@code cohere.embed-v4.0})
     * @param endpointId        the OCID of a dedicated AI cluster endpoint, or {@code null} for on-demand serving
     * @param uri               an explicit base URL overriding the public regional endpoint, or {@code null}
     * @param rateLimitSettings the rate limit, never {@code null}
     */
    public record CommonSettings(
        @Nullable String region,
        String compartmentId,
        String modelId,
        @Nullable String endpointId,
        @Nullable URI uri,
        RateLimitSettings rateLimitSettings
    ) {
        public CommonSettings {
            Objects.requireNonNull(compartmentId);
            Objects.requireNonNull(modelId);
            rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
            if (region == null && uri == null) {
                throw new IllegalArgumentException(Strings.format("Either [%s] or [%s] must be provided", REGION, URL));
            }
        }

        public CommonSettings(StreamInput in) throws IOException {
            this(
                in.readOptionalString(),
                in.readString(),
                in.readString(),
                in.readOptionalString(),
                ServiceUtils.createOptionalUri(in.readOptionalString()),
                new RateLimitSettings(in)
            );
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(region);
            out.writeString(compartmentId);
            out.writeString(modelId);
            out.writeOptionalString(endpointId);
            out.writeOptionalString(uri == null ? null : uri.toString());
            rateLimitSettings.writeTo(out);
        }

        public CommonSettings withRateLimitSettings(RateLimitSettings newRateLimitSettings) {
            return new CommonSettings(region, compartmentId, modelId, endpointId, uri, newRateLimitSettings);
        }

        public CommonSettings withModelId(String newModelId) {
            return new CommonSettings(region, compartmentId, newModelId, endpointId, uri, rateLimitSettings);
        }
    }

    /**
     * Extracts (and removes) the common fields from the settings map, accumulating validation errors.
     *
     * @return the common settings, or {@code null} if a required field is missing (in which case the validation exception carries the
     *         error)
     */
    @Nullable
    protected static CommonSettings extractCommonSettings(
        Map<String, Object> map,
        ValidationException validationException,
        ConfigurationParseContext context
    ) {
        var region = extractOptionalString(map, REGION, SERVICE_SETTINGS, validationException);
        var uri = extractOptionalUri(map, URL, validationException);
        var compartmentId = extractRequiredString(map, COMPARTMENT_ID, SERVICE_SETTINGS, validationException);
        var modelId = extractRequiredString(map, MODEL_ID, SERVICE_SETTINGS, validationException);
        var endpointId = extractOptionalString(map, ENDPOINT_ID, SERVICE_SETTINGS, validationException);
        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, context);

        if (region == null && uri == null) {
            validationException.addValidationError(
                Strings.format("[%s] must contain either the [%s] or the [%s] setting", SERVICE_SETTINGS, REGION, URL)
            );
        } else if (region != null && REGION_PATTERN.matcher(region).matches() == false) {
            validationException.addValidationError(
                Strings.format(
                    "[%s] Invalid value [%s] for [%s]. It must be an OCI region identifier such as [us-chicago-1]",
                    SERVICE_SETTINGS,
                    region,
                    REGION
                )
            );
        }

        if (validationException.validationErrors().isEmpty() == false) {
            return null;
        }
        return new CommonSettings(region, compartmentId, modelId, endpointId, uri, rateLimitSettings);
    }

    private final CommonSettings common;

    protected OciGenAiServiceSettings(CommonSettings common) {
        this.common = Objects.requireNonNull(common);
    }

    public CommonSettings common() {
        return common;
    }

    @Nullable
    public String region() {
        return common.region();
    }

    public String compartmentId() {
        return common.compartmentId();
    }

    @Override
    public String modelId() {
        return common.modelId();
    }

    @Nullable
    public String endpointId() {
        return common.endpointId();
    }

    @Nullable
    public URI uri() {
        return common.uri();
    }

    public RateLimitSettings rateLimitSettings() {
        return common.rateLimitSettings();
    }

    /**
     * @return {@code true} if requests target a dedicated AI cluster endpoint instead of on-demand serving
     */
    public boolean isDedicated() {
        return common.endpointId() != null;
    }

    public OciGenAiChatApiFormat apiFormat() {
        return OciGenAiChatApiFormat.fromModelId(common.modelId());
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return OciGenAiUtils.ML_INFERENCE_OCI_GENAI_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(OciGenAiUtils.ML_INFERENCE_OCI_GENAI_ADDED);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (common.region() != null) {
            builder.field(REGION, common.region());
        }
        builder.field(COMPARTMENT_ID, common.compartmentId());
        builder.field(MODEL_ID, common.modelId());
        if (common.endpointId() != null) {
            builder.field(ENDPOINT_ID, common.endpointId());
        }
        if (common.uri() != null) {
            builder.field(URL, common.uri().toString());
        }
        common.rateLimitSettings().toXContent(builder, params);
        return builder;
    }

    @Override
    public String toString() {
        return org.elasticsearch.common.Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OciGenAiServiceSettings that = (OciGenAiServiceSettings) o;
        return Objects.equals(common, that.common);
    }

    @Override
    public int hashCode() {
        return Objects.hash(common);
    }
}
