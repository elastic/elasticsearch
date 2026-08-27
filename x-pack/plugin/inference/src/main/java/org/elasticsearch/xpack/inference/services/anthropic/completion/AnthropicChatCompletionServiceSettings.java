/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.completion;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.ServiceSettingsOPBuilder;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.common.parser.UpdateServiceSettingsOPBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;

/**
 * Defines the service settings for interacting with Anthropic's chat completion models.
 */
public class AnthropicChatCompletionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        AnthropicRateLimitServiceSettings {

    public static final String NAME = "anthropic_completion_service_settings";

    static final TransportVersion ANTHROPIC_COMPLETION_URL_ADDED = TransportVersion.fromName(
        "inference_api_anthropic_completion_url_added"
    );

    // The rate limit for build tier 1 is 50 request per minute
    // Details are here https://docs.anthropic.com/en/api/rate-limits
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(50);

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the Anthropic chat completion service settings.
     *
     * @param ignoreUnknownFields {@code false} for request parsing (reject unexpected fields),
     *                            {@code true} for persisted configuration (tolerate fields from other versions).
     */
    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        var parser = ServiceSettingsOPBuilder.of(
            ignoreUnknownFields,
            Builder::new,
            DEFAULT_RATE_LIMIT_SETTINGS,
            Builder::setRateLimitSettings
        ).build();
        parser.declareString(Builder::setModelId, new ParseField(MODEL_ID));
        parser.declareString(Builder::setUrl, new ParseField(URL));
        return parser;
    }

    static class Builder {
        private String modelId;
        private String url;
        private RateLimitSettings rateLimitSettings;

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        public AnthropicChatCompletionServiceSettings build() {
            validateStringIsNotNullOrEmpty(modelId, MODEL_ID);
            if (url != null) {
                validateStringIsNotNullOrEmpty(url, URL);
            }
            return new AnthropicChatCompletionServiceSettings(modelId, createOptionalUri(url), rateLimitSettings);
        }
    }

    public static AnthropicChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, context).build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    private final String modelId;
    @Nullable
    private final URI url;
    private final RateLimitSettings rateLimitSettings;

    public AnthropicChatCompletionServiceSettings(String modelId, @Nullable URI url, @Nullable RateLimitSettings ratelimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.url = url;
        this.rateLimitSettings = Objects.requireNonNullElse(ratelimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public AnthropicChatCompletionServiceSettings(String modelId, @Nullable RateLimitSettings ratelimitSettings) {
        this(modelId, null, ratelimitSettings);
    }

    public AnthropicChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.url = in.getTransportVersion().supports(ANTHROPIC_COMPLETION_URL_ADDED) ? createOptionalUri(in.readOptionalString()) : null;
        rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Nullable
    public URI url() {
        return url;
    }

    @Override
    public AnthropicChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse Anthropic chat completion service settings update", e);
        }

    }

    /**
     * Parses an update request, which may only contain the mutable {@code rate_limit} field.
     * Including any immutable field (such as {@code model_id} or {@code url}) causes the strict parser to reject the request.
     */
    private static class Update {

        private static final ObjectParser<Update, Void> PARSER = UpdateServiceSettingsOPBuilder.of(
            Update::new,
            Update::setRateLimitSettings
        ).build();

        private StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();

        private void setRateLimitSettings(StatefulValue<RateLimitSettings> rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        public AnthropicChatCompletionServiceSettings mergeInto(AnthropicChatCompletionServiceSettings existing) {
            return new AnthropicChatCompletionServiceSettings(
                existing.modelId(),
                existing.url(),
                applyUpdate(rateLimitSettings, existing.rateLimitSettings(), DEFAULT_RATE_LIMIT_SETTINGS)
            );
        }
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
        builder.field(MODEL_ID, modelId);
        if (url != null) {
            builder.field(URL, url.toString());
        }
        rateLimitSettings.toXContent(builder, params);

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
        out.writeString(modelId);
        if (out.getTransportVersion().supports(ANTHROPIC_COMPLETION_URL_ADDED)) {
            out.writeOptionalString(url != null ? url.toString() : null);
        }
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        AnthropicChatCompletionServiceSettings that = (AnthropicChatCompletionServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(url, that.url)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, url, rateLimitSettings);
    }
}
