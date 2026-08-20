/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;

/**
 * Abstract base for all TencentCloud task-specific service settings. Holds the fields shared across every TencentCloud task
 * (model identity, region, and rate limiting) together with the parsing, serialization, and update machinery that would
 * otherwise be duplicated. Task-specific subclasses contribute only their own additional fields.
 * <p>
 * The endpoint URL is not user-configurable; it is always constructed from the region and the task-specific path
 * ({@code https://{region}.aisearch.tencentelasticsearch.com/v1/<task-path>}).
 */
public abstract class TencentCloudCommonServiceSettings extends FilteredXContentObject implements ServiceSettings {

    private static final String REGION = "region";

    // Default rate limit for TencentCloud AI Gateway (see docs).
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(20);

    /**
     * Declares the common TencentCloud service-settings fields ({@code model_id}, {@code region}, {@code rate_limit})
     * onto the given parser so that every task-specific settings parser can reuse the same declaration.
     */
    public static <B extends Builder<? extends TencentCloudCommonServiceSettings>> void declareCommonFields(
        AbstractObjectParser<B, ConfigurationParseContext> parser,
        RateLimitSettings defaultRateLimit
    ) {
        parser.declareString(Builder::setModelId, new ParseField(ServiceFields.MODEL_ID));
        parser.declareString(Builder::setRegion, new ParseField(REGION));
        parser.declareObject(
            Builder::setRateLimitSettings,
            (p, c) -> RateLimitSettings.createParser(c == ConfigurationParseContext.PERSISTENT, defaultRateLimit).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME)
        );
        // api_key appears in the same JSON block as service settings in REST requests; DefaultSecretSettings extracts it separately.
        // Declare it here as a no-op so the strict REQUEST parser does not reject it as an unknown field.
        parser.declareString((b, v) -> {}, new ParseField(DefaultSecretSettings.API_KEY));
    }

    /**
     * Parses common settings from a map using the given parser, returning the fully constructed task-specific settings.
     *
     * @param map     the map to parse
     * @param context the context in which the parsing is done
     * @param parser  the parser to use for parsing the settings
     * @return the created settings
     */
    public static <T extends TencentCloudCommonServiceSettings> T fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ObjectParser<? extends Builder<T>, ConfigurationParseContext> parser
    ) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, context).build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    /**
     * Accumulates the parsed common fields on behalf of a concrete settings builder. Each task-specific builder extends this and
     * contributes its own fields, implementing {@link #build(String, String, RateLimitSettings)} to assemble the final settings object.
     *
     * @param <T> the task-specific settings type produced by {@link #build(String, String, RateLimitSettings)}
     */
    public abstract static class Builder<T extends TencentCloudCommonServiceSettings> {
        private String modelId;
        private String region;
        protected RateLimitSettings rateLimitSettings;

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setRegion(String region) {
            this.region = region;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        protected abstract T build(String modelId, String region, RateLimitSettings rateLimitSettings);

        public final T build() {
            validateStringIsNotNullOrEmpty(modelId, ServiceFields.MODEL_ID);
            return build(modelId, region, rateLimitSettings);
        }
    }

    /**
     * Registers the common TencentCloud fields that may be changed by an update request. Only {@code rate_limit} is mutable; the
     * immutable fields (such as {@code model_id} and {@code region}) are intentionally not declared so that a strict update parser
     * rejects attempts to change them.
     */
    public static void declareCommonUpdatableFields(AbstractObjectParser<? extends CommonUpdate, Void> parser) {
        StatefulValue.declareNullable(
            parser,
            (update, value) -> update.rateLimitSettings = value,
            (p) -> RateLimitSettings.createParser(false, null).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME),
            ObjectParser.ValueType.OBJECT_OR_NULL
        );
    }

    /**
     * Common fields parsed from an update request. Because settings are immutable, each subclass builds the new instance itself,
     * calling {@link #mergedRateLimitSettings(TencentCloudCommonServiceSettings)} to resolve the shared fields.
     */
    public static class CommonUpdate {

        protected StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();

        /**
         * Resolves the rate limit settings to use after applying the update following the tri-state convention: an omitted field keeps
         * the current value, an explicit null resets the field to the default rate limit, and a present value replaces the current one.
         */
        protected RateLimitSettings mergedRateLimitSettings(TencentCloudCommonServiceSettings existing) {
            return applyUpdate(rateLimitSettings, existing.rateLimitSettings(), DEFAULT_RATE_LIMIT_SETTINGS);
        }
    }

    private final String modelId;
    private final String region;
    private final RateLimitSettings rateLimitSettings;

    protected TencentCloudCommonServiceSettings(String modelId, String region, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.region = region != null && region.isBlank() == false ? region : TencentCloudUtils.DEFAULT_REGION;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    protected TencentCloudCommonServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.region = in.readString();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public String modelId() {
        return modelId;
    }

    public String region() {
        return region;
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    /**
     * Applies an update to the mutable fields of these settings, returning a copy. Only the rate limit is mutable; the model id and
     * region are immutable and are carried over unchanged. Concrete subclasses override this to also preserve their task-specific
     * fields.
     */
    @Override
    public abstract TencentCloudCommonServiceSettings updateServiceSettings(Map<String, Object> serviceSettings);

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TencentCloudService.TENCENT_CLOUD_INFERENCE_SERVICE_ADDED;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(ServiceFields.MODEL_ID, modelId);
        builder.field(REGION, region);
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeString(region);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TencentCloudCommonServiceSettings that = (TencentCloudCommonServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(region, that.region)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, region, rateLimitSettings);
    }
}
