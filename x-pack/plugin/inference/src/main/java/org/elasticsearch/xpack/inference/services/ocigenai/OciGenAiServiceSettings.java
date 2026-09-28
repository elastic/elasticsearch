/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.ServiceSettingsOPBuilder;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.common.parser.UpdateServiceSettingsOPBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.core.inference.InferenceUtils.mustBeNonEmptyString;
import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.SettingsScope.SERVICE_SETTINGS;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiSecretSettings.FINGERPRINT;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiSecretSettings.PRIVATE_KEY;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiSecretSettings.TENANCY_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiSecretSettings.USER_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.COMPARTMENT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.ENDPOINT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.REGION;

/**
 * Base class for the OCI Generative AI task specific service settings. Holds the fields shared by every task: the region (or an
 * explicit endpoint URL), the compartment OCID, the model id, an optional dedicated AI cluster endpoint OCID, the inference API
 * version and the rate limit. It also provides the {@link ObjectParser} building blocks the task specific subclasses extend with their
 * own fields.
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
     * Builds an {@link ObjectParser} for OCI Generative AI service settings, wiring the common fields ({@code region}, {@code url},
     * {@code compartment_id}, {@code model_id}, {@code endpoint_id}, {@code api_version}) and {@link #DEFAULT_RATE_LIMIT_SETTINGS}.
     * The OCI API signing key fields are declared as no-ops because, in requests, they share the {@code service_settings} block with
     * the fields parsed here and are extracted separately by {@link OciGenAiSecretSettings}.
     */
    public static <B extends Builder<? extends OciGenAiServiceSettings>> ObjectParser<B, ConfigurationParseContext> buildCommonParser(
        boolean ignoreUnknownFields,
        Supplier<B> builderSupplier
    ) {
        var parser = new ServiceSettingsOPBuilder<>(ignoreUnknownFields, builderSupplier).enableRateLimitSettings(
            Builder::setRateLimitSettings,
            DEFAULT_RATE_LIMIT_SETTINGS
        ).allowSecretFields(TENANCY_ID, USER_ID, FINGERPRINT, PRIVATE_KEY).build();
        parser.declareString(Builder::setRegion, new ParseField(REGION));
        parser.declareString(Builder::setUrl, new ParseField(URL));
        parser.declareString(Builder::setCompartmentId, new ParseField(COMPARTMENT_ID));
        parser.declareString(Builder::setModelId, new ParseField(MODEL_ID));
        parser.declareString(Builder::setEndpointId, new ParseField(ENDPOINT_ID));
        parser.declareString(Builder::setApiVersion, new ParseField(API_VERSION));
        return parser;
    }

    /**
     * Builds an {@link ObjectParser} for OCI Generative AI update requests, wiring the rate limit and the signing key no-ops (the
     * signing key can be rotated through the same update request). The immutable fields are intentionally not declared so that the
     * strict update parser rejects attempts to change them.
     */
    public static <U extends CommonUpdate> ObjectParser<U, Void> buildCommonUpdateParser(Supplier<U> updateSupplier) {
        return new UpdateServiceSettingsOPBuilder<>(updateSupplier).enableRateLimitSettings(CommonUpdate::setRateLimitSettings)
            .allowSecretFields(TENANCY_ID, USER_ID, FINGERPRINT, PRIVATE_KEY)
            .build();
    }

    /**
     * Creates the task specific service settings from a map of settings using the given parser.
     *
     * @param map     the settings to parse
     * @param context the context in which the parsing is done
     * @param parser  the parser matching the context, see {@link #buildCommonParser(boolean, Supplier)}
     */
    public static <T extends OciGenAiServiceSettings> T fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ObjectParser<? extends Builder<T>, ConfigurationParseContext> parser
    ) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, context).build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, SERVICE_SETTINGS);
        }
    }

    /**
     * Parses an update request with the given update parser, see {@link #buildCommonUpdateParser(Supplier)}.
     */
    protected static <U extends CommonUpdate> U parseUpdate(Map<String, Object> serviceSettings, ObjectParser<U, Void> parser) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return parser.apply(xParser, null);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse the [{}] update", e, SERVICE_SETTINGS);
        }
    }

    /**
     * The settings shared by all OCI Generative AI tasks.
     *
     * @param region            the OCI region identifier; may be {@code null} only when {@code uri} is provided
     * @param compartmentId     the compartment OCID
     * @param modelId           the OCI Generative AI model id (for example {@code cohere.embed-v4.0})
     * @param endpointId        the OCID of a dedicated AI cluster endpoint, or {@code null} for on-demand serving
     * @param uri               an explicit base URL overriding the public regional endpoint, or {@code null}
     * @param apiVersion        the inference API version, defaults to {@link OciGenAiUtils#DEFAULT_API_VERSION} when {@code null}
     * @param rateLimitSettings the rate limit, defaults to {@link #DEFAULT_RATE_LIMIT_SETTINGS} when {@code null}
     */
    public record CommonSettings(
        @Nullable String region,
        String compartmentId,
        String modelId,
        @Nullable String endpointId,
        @Nullable URI uri,
        String apiVersion,
        RateLimitSettings rateLimitSettings
    ) {
        public CommonSettings {
            Objects.requireNonNull(compartmentId);
            Objects.requireNonNull(modelId);
            apiVersion = Objects.requireNonNullElse(apiVersion, OciGenAiUtils.DEFAULT_API_VERSION);
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
                createOptionalUri(in.readOptionalString()),
                in.readString(),
                new RateLimitSettings(in)
            );
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(region);
            out.writeString(compartmentId);
            out.writeString(modelId);
            out.writeOptionalString(endpointId);
            out.writeOptionalString(uri == null ? null : uri.toString());
            out.writeString(apiVersion);
            rateLimitSettings.writeTo(out);
        }

        public CommonSettings withRateLimitSettings(RateLimitSettings newRateLimitSettings) {
            return new CommonSettings(region, compartmentId, modelId, endpointId, uri, apiVersion, newRateLimitSettings);
        }

        public CommonSettings withModelId(String newModelId) {
            return new CommonSettings(region, compartmentId, newModelId, endpointId, uri, apiVersion, rateLimitSettings);
        }
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

    public String apiVersion() {
        return common.apiVersion();
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
        return OciGenAiUtils.INFERENCE_OCI_GENAI_ADDED;
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
        builder.field(API_VERSION, common.apiVersion());
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

    /**
     * Accumulates the parsed common fields and assembles the {@link CommonSettings}, enforcing that {@code compartment_id} and
     * {@code model_id} are present and that either {@code region} or {@code url} is provided. Task specific builders extend this
     * and contribute their own fields.
     *
     * @param <T> the task specific settings type produced by {@link #build(CommonSettings)}
     */
    public abstract static class Builder<T extends OciGenAiServiceSettings> {

        private String region;
        private String url;
        private String compartmentId;
        private String modelId;
        private String endpointId;
        private String apiVersion;
        protected RateLimitSettings rateLimitSettings;

        public void setRegion(String region) {
            this.region = region;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public void setCompartmentId(String compartmentId) {
            this.compartmentId = compartmentId;
        }

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setEndpointId(String endpointId) {
            this.endpointId = endpointId;
        }

        public void setApiVersion(String apiVersion) {
            this.apiVersion = apiVersion;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        protected abstract T build(CommonSettings common);

        public final T build() {
            validateStringIsNotNullOrEmpty(compartmentId, COMPARTMENT_ID);
            validateStringIsNotNullOrEmpty(modelId, MODEL_ID);
            validateOptionalStringIsNotEmpty(url, URL);
            validateOptionalStringIsNotEmpty(endpointId, ENDPOINT_ID);
            validateOptionalStringIsNotEmpty(apiVersion, API_VERSION);

            if (region == null && url == null) {
                throw new IllegalArgumentException(
                    Strings.format("[%s] must contain either the [%s] or the [%s] setting", SERVICE_SETTINGS, REGION, URL)
                );
            }
            if (region != null && REGION_PATTERN.matcher(region).matches() == false) {
                throw new IllegalArgumentException(
                    Strings.format(
                        "[%s] Invalid value [%s] for [%s]. It must be an OCI region identifier such as [us-chicago-1]",
                        SERVICE_SETTINGS,
                        region,
                        REGION
                    )
                );
            }

            return build(
                new CommonSettings(region, compartmentId, modelId, endpointId, createOptionalUri(url), apiVersion, rateLimitSettings)
            );
        }

        private static void validateOptionalStringIsNotEmpty(@Nullable String value, String settingName) {
            if (value != null && value.isEmpty()) {
                throw new IllegalArgumentException(mustBeNonEmptyString(settingName, SERVICE_SETTINGS.toString()));
            }
        }
    }

    /**
     * Common fields parsed from an update request. Because settings are immutable, each subclass builds the new instance itself,
     * calling {@link #mergedRateLimitSettings(OciGenAiServiceSettings)} to resolve the shared fields.
     */
    public static class CommonUpdate {

        protected StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();

        protected void setRateLimitSettings(StatefulValue<RateLimitSettings> rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        /**
         * Resolves the rate limit settings to use after applying the update following the tri-state convention: an omitted field keeps
         * the current value, an explicit null resets the field to the default rate limit, and a present value replaces the current one.
         */
        protected RateLimitSettings mergedRateLimitSettings(OciGenAiServiceSettings existing) {
            return applyUpdate(rateLimitSettings, existing.rateLimitSettings(), DEFAULT_RATE_LIMIT_SETTINGS);
        }
    }
}
