/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.completion;

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
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.groq.GroqRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.groq.GroqService;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;

/**
 * Service settings for Groq chat completion models.
 * Groq reuses the OpenAI wire format, so this largely mirrors the OpenAI settings class
 * but applies Groq-specific defaults such as the base URL and rate limits documented in
 * <a href="https://console.groq.com/docs/rate-limits">Groq Documentation</a>.
 */
public class GroqChatCompletionServiceSettings extends FilteredXContentObject implements ServiceSettings, GroqRateLimitServiceSettings {

    public static final String NAME = "groq_completion_service_settings";

    // The rate limit for dev tier depends on the model used. For example, the rate limit for the `openai/gpt-oss-20b` model is 1,000
    // requests per minute.
    // To find this information you need to access your account's limits https://console.groq.com/docs/rate-limits.
    static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1_000);

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the Groq chat completion service settings.
     *
     * @param ignoreUnknownFields whether the parser should tolerate unknown fields. This is {@code false} for request parsing (so that
     *                            unexpected fields are rejected) and {@code true} for persisted configuration (so that fields written by
     *                            other versions are tolerated).
     * @return the parser
     */
    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            ignoreUnknownFields,
            Builder::new
        );
        parser.declareString(Builder::setModelId, new ParseField(ServiceFields.MODEL_ID));
        parser.declareString(Builder::setUrl, new ParseField(ServiceFields.URL));
        parser.declareString(Builder::setOrganizationId, new ParseField(OpenAiServiceFields.ORGANIZATION));
        parser.declareObject(
            Builder::setRateLimitSettings,
            (p, c) -> RateLimitSettings.createParser(c == ConfigurationParseContext.PERSISTENT, DEFAULT_RATE_LIMIT_SETTINGS).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME)
        );
        // api_key appears in the same JSON block as service settings in REST requests; DefaultSecretSettings extracts it separately.
        // Declare it here as a no-op so the strict REQUEST parser does not reject it as an unknown field.
        parser.declareString((b, v) -> {}, new ParseField(DefaultSecretSettings.API_KEY));
        return parser;
    }

    /**
     * Creates a new instance from a map of settings.
     *
     * @param map     the map containing the service settings
     * @param context the context for parsing configuration settings
     * @return a new instance of {@link GroqChatCompletionServiceSettings}
     */
    public static GroqChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, context).build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    private final String modelId;
    @Nullable
    private final URI uri;
    @Nullable
    private final String organizationId;
    private final RateLimitSettings rateLimitSettings;

    public GroqChatCompletionServiceSettings(
        String modelId,
        @Nullable URI uri,
        @Nullable String organizationId,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.modelId = Objects.requireNonNull(modelId);
        this.uri = uri;
        this.organizationId = organizationId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public GroqChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.uri = ServiceUtils.createOptionalUri(in.readOptionalString());
        this.organizationId = in.readOptionalString();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public GroqChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse Groq chat completion service settings update", e);
        }
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public URI uri() {
        return uri;
    }

    @Override
    public String organizationId() {
        return organizationId;
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
        if (uri != null) {
            builder.field(ServiceFields.URL, uri.toString());
        }
        if (organizationId != null) {
            builder.field(OpenAiServiceFields.ORGANIZATION, organizationId);
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
        return GroqService.GROQ_INFERENCE_SERVICE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalString(uri != null ? uri.toString() : null);
        out.writeOptionalString(organizationId);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        GroqChatCompletionServiceSettings that = (GroqChatCompletionServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(organizationId, that.organizationId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, organizationId, rateLimitSettings);
    }

    /**
     * Accumulates the parsed fields and assembles a {@link GroqChatCompletionServiceSettings}, enforcing that the required
     * {@code model_id} field is present.
     */
    public static class Builder {

        private String modelId;
        private String url;
        private String organizationId;
        private RateLimitSettings rateLimitSettings;

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public void setOrganizationId(String organizationId) {
            this.organizationId = organizationId;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        public GroqChatCompletionServiceSettings build() {
            validateStringIsNotNullOrEmpty(modelId, ServiceFields.MODEL_ID);
            URI uri = url != null ? ServiceUtils.createUri(url) : null;
            return new GroqChatCompletionServiceSettings(modelId, uri, organizationId, rateLimitSettings);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code organization_id} and {@code rate_limit} fields. Including
     * any immutable field causes the strict parser to reject the request.
     */
    private static class Update {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            StatefulValue.declareNullable(PARSER, (update, value) -> update.organizationId = value, p -> {
                var value = p.text();
                validateStringIsNotNullOrEmpty(value, OpenAiServiceFields.ORGANIZATION);
                return value;
            }, new ParseField(OpenAiServiceFields.ORGANIZATION), ObjectParser.ValueType.STRING_OR_NULL);
            RateLimitSettings.declareUpdatableRateLimitSettings(PARSER, Update::setRateLimitSettings);
            // api_key appears in the same JSON block as service settings in update requests; DefaultSecretSettings extracts it separately.
            // Declare it here as a no-op so the strict parser does not reject it as an unknown field.
            PARSER.declareString((u, v) -> {}, new ParseField(DefaultSecretSettings.API_KEY));
        }

        private StatefulValue<String> organizationId = StatefulValue.undefined();
        private StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();

        private void setRateLimitSettings(StatefulValue<RateLimitSettings> rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        public GroqChatCompletionServiceSettings mergeInto(GroqChatCompletionServiceSettings existing) {
            return new GroqChatCompletionServiceSettings(
                existing.modelId,
                existing.uri,
                applyUpdate(organizationId, existing.organizationId),
                applyUpdate(rateLimitSettings, existing.rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS)
            );
        }
    }
}
