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

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

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

    /**
     * Declares the common TencentCloud service-settings fields ({@code model_id}, {@code url}, {@code rate_limit},
     * and a no-op {@code api_key}) onto the given parser so that every task-specific settings parser can reuse the
     * same declaration.
     */
    public static <B extends CommonSettingsBuilder> void declareCommonFields(
        AbstractObjectParser<B, ConfigurationParseContext> parser,
        RateLimitSettings defaultRateLimit
    ) {
        parser.declareString((b, v) -> b.setModelId(v), new ParseField(ServiceFields.MODEL_ID));
        parser.declareString((b, v) -> b.setUrl(v), new ParseField(ServiceFields.URL));
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
     * Parses common settings from a map using an ObjectParser. This is the recommended way to parse task-level
     * settings with their task-specific parser; it replaces the previous hand-written map extraction.
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
            // SSRF guard: only enforce the allow-list when the caller creates/updates an endpoint via the REST API.
            if (context == ConfigurationParseContext.REQUEST) {
                TencentCloudEndpointUtils.validateEndpoint(
                    commonSettings.uri(),
                    ServiceFields.URL,
                    ModelConfigurations.SERVICE_SETTINGS,
                    validationException
                );
                if (validationException.validationErrors().isEmpty() == false) {
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

        void setUrl(String url);

        void setRateLimitSettings(RateLimitSettings rateLimitSettings);

        TencentCloudCommonServiceSettings buildCommon();
    }

    // ---- instance fields and methods ----

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
        return TencentCloudService.TENCENT_CLOUD_INFERENCE_SERVICE_ADDED;
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
