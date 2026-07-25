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
 * Common service settings shared by all TencentCloud task types.
 * Contains the required {@code model_id}, an optional {@code region} (defaults to {@code bj}), and rate limit settings.
 * <p>
 * The endpoint URL is not user-configurable; it is always constructed from the region and the task-specific path
 * ({@code https://{region}.aisearch.tencentelasticsearch.com/v1/<task-path>}).
 */
public class TencentCloudCommonServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        TencentCloudRateLimitServiceSettings {

    public static final String NAME = "tencentcloud_service_settings";
    private static final String REGION = "region";

    // Default rate limit for TencentCloud AI Gateway (see docs).
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(20);

    /**
     * Declares the common TencentCloud service-settings fields ({@code model_id}, {@code region}, {@code rate_limit},
     * and a no-op {@code api_key}) onto the given parser so that every task-specific settings parser can reuse the
     * same declaration. The {@code url} field (if present) is silently consumed for backward compatibility with
     * persisted configurations.
     */
    public static <B extends CommonSettingsBuilder> void declareCommonFields(
        AbstractObjectParser<B, ConfigurationParseContext> parser,
        RateLimitSettings defaultRateLimit
    ) {
        parser.declareString((b, v) -> b.setModelId(v), new ParseField(ServiceFields.MODEL_ID));
        parser.declareString((b, v) -> b.setRegion(v), new ParseField(REGION));
        // Consume the legacy url field silently so that persisted configurations from older versions don't fail to parse.
        parser.declareString((b, v) -> {}, new ParseField(ServiceFields.URL));
        parser.declareObject(
            (b, v) -> b.setRateLimitSettings(v),
            (p, c) -> RateLimitSettings.createParser(c == ConfigurationParseContext.PERSISTENT, defaultRateLimit).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME)
        );
        // api_key appears in the same JSON block as service settings in REST requests; DefaultSecretSettings extracts
        // it separately. Declare it here as a no-op so the strict REQUEST parser does not reject it as an unknown field.
        parser.declareString((b, v) -> {}, new ParseField(DefaultSecretSettings.API_KEY));
    }

    /**
     * Parses common settings from a map using an ObjectParser.
     */
    public static <T extends CommonSettingsBuilder> TencentCloudCommonServiceSettings fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ObjectParser<T, ConfigurationParseContext> parser,
        ValidationException validationException
    ) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            T builder = parser.apply(xParser, context);
            var commonSettings = builder.buildCommon();
            // Validate region in REQUEST context.
            if (context == ConfigurationParseContext.REQUEST) {
                if (commonSettings.region().isBlank()) {
                    validationException.addValidationError(
                        String.format(Locale.ROOT, "[%s] in [%s] must not be empty", REGION, ModelConfigurations.SERVICE_SETTINGS)
                    );
                    return null;
                }
            }
            return commonSettings;
        } catch (ElasticsearchParseException e) {
            validationException.addValidationError(e.getMessage());
            return null;
        } catch (IOException e) {
            validationException.addValidationError("Failed to parse TencentCloud service settings: " + e.getMessage());
            return null;
        }
    }

    /**
     * Builder interface for classes that accumulate common TencentCloud settings fields.
     */
    public interface CommonSettingsBuilder {
        void setModelId(String modelId);

        void setRegion(String region);

        void setRateLimitSettings(RateLimitSettings rateLimitSettings);

        TencentCloudCommonServiceSettings buildCommon();
    }

    // ---- instance fields and methods ----

    private final String modelId;
    private final String region;
    private final RateLimitSettings rateLimitSettings;

    public TencentCloudCommonServiceSettings(String modelId, @Nullable String region, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.region = region != null && region.isBlank() == false ? region : TencentCloudUtils.DEFAULT_REGION;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public TencentCloudCommonServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.region = in.readString();
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

        return new TencentCloudCommonServiceSettings(this.modelId, this.region, extractedRateLimitSettings);
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
        builder.field(REGION, region);
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TencentCloudService.TENCENT_CLOUD_INFERENCE_SERVICE_ADDED;
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
