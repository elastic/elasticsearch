/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
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
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudUtils;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract base for all TencentCloud task-specific service settings. Holds the fields shared across every TencentCloud task
 * (model identity, region, and rate limiting) together with the parsing, serialization, and update machinery that would
 * otherwise be duplicated. Task-specific subclasses contribute only their own additional fields.
 * <p>
 * The endpoint URL is not user-configurable; it is always constructed from the region and the task-specific path
 * ({@code https://{region}.aisearch.tencentelasticsearch.com/v1/<task-path>}).
 */
public abstract class TencentCloudCommonServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        TencentCloudRateLimitServiceSettings {

    public static final String NAME = "tencentcloud_service_settings";
    private static final String REGION = "region";

    // Default rate limit for TencentCloud AI Gateway (see docs).
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(20);

    /**
     * Declares the common TencentCloud service-settings fields ({@code model_id}, {@code region}, {@code rate_limit},
     * and a no-op {@code url}) onto the given parser so that every task-specific settings parser can reuse the
     * same declaration. The {@code url} field (if present) is silently consumed for backward compatibility with
     * persisted configurations.
     */
    public static <B extends Builder<? extends TencentCloudCommonServiceSettings>> void declareCommonFields(
        AbstractObjectParser<B, ConfigurationParseContext> parser,
        RateLimitSettings defaultRateLimit
    ) {
        parser.declareString(Builder::setModelId, new ParseField(ServiceFields.MODEL_ID));
        parser.declareString(Builder::setRegion, new ParseField(REGION));
        // Consume the legacy url field silently so that persisted configurations from older versions don't fail to parse.
        parser.declareString((b, v) -> {}, new ParseField(ServiceFields.URL));
        parser.declareObject(
            Builder::setRateLimitSettings,
            (p, c) -> RateLimitSettings.createParser(c == ConfigurationParseContext.PERSISTENT, defaultRateLimit).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME)
        );
        // api_key appears in the same JSON block as service settings in REST requests; DefaultSecretSettings extracts
        // it separately. Declare it here as a no-op so the strict REQUEST parser does not reject it as an unknown field.
        parser.declareString((b, v) -> {}, new ParseField(DefaultSecretSettings.API_KEY));
    }

    /**
     * Parses common settings from a map using the given parser, returning the fully constructed task-specific settings.
     *
     * @param map                  the map to parse
     * @param context              the context in which the parsing is done
     * @param parser               the parser to use for parsing the settings
     * @param validationException  the validation exception to populate in case of errors
     * @param <T>                  the concrete settings type produced by the parser's builder
     * @return the created settings, or {@code null} if a validation error occurred
     */
    public static <T extends TencentCloudCommonServiceSettings> T fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ObjectParser<? extends Builder<T>, ConfigurationParseContext> parser,
        ValidationException validationException
    ) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            T settings = parser.apply(xParser, context).build();
            // Validate region in REQUEST context.
            if (context == ConfigurationParseContext.REQUEST) {
                if (settings.region().isBlank()) {
                    validationException.addValidationError(
                        String.format(Locale.ROOT, "[%s] in [%s] must not be empty", REGION, ModelConfigurations.SERVICE_SETTINGS)
                    );
                    return null;
                }
            }
            return settings;
        } catch (ElasticsearchParseException e) {
            validationException.addValidationError(e.getMessage());
            return null;
        } catch (IOException e) {
            validationException.addValidationError("Failed to parse TencentCloud service settings: " + e.getMessage());
            return null;
        }
    }

    /**
     * Accumulates the parsed common fields on behalf of a concrete settings builder. Each task-specific builder extends this and
     * contributes its own fields, implementing {@link #build()} to assemble the final settings object.
     *
     * @param <T> the task-specific settings type produced by {@link #build()}
     */
    public abstract static class Builder<T extends TencentCloudCommonServiceSettings> {
        protected String modelId;
        protected String region;
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

        protected abstract T build();
    }

    // ---- instance fields and methods ----

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

    @Override
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
