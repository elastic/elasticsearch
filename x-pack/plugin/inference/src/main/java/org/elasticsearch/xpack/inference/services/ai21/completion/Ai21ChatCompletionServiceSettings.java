/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21.completion;

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
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;

/**
 * Represents the settings for the AI21 chat completion service.
 * This class encapsulates the model ID and rate limit settings for the AI21 chat completion service.
 */
public class Ai21ChatCompletionServiceSettings extends FilteredXContentObject implements ServiceSettings {
    public static final String NAME = "ai21_completions_service_settings";

    // Rate limit for AI21 is 10 requests / sec or 200 requests / minute. Setting default to 200 requests / minute
    static final int DEFAULT_REQUESTS_PER_MINUTE = 200;
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(DEFAULT_REQUESTS_PER_MINUTE);

    private static final TransportVersion ML_INFERENCE_AI21_COMPLETION_ADDED = TransportVersion.fromName(
        "ml_inference_ai21_completion_added"
    );

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the AI21 chat completion service settings.
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
        parser.declareString(Builder::setModelId, new ParseField(MODEL_ID));
        RateLimitSettings.declareRateLimitSettings(parser, Builder::setRateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
        // api_key appears in the same JSON block as service settings in REST requests; DefaultSecretSettings extracts it separately.
        // Declare it here as a no-op so the strict REQUEST parser does not reject it as an unknown field.
        parser.declareString((b, v) -> {}, new ParseField(DefaultSecretSettings.API_KEY));
        return parser;
    }

    static class Builder {
        private String modelId;
        private RateLimitSettings rateLimitSettings;

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        public Ai21ChatCompletionServiceSettings build() {
            validateStringIsNotNullOrEmpty(modelId, MODEL_ID);
            return new Ai21ChatCompletionServiceSettings(modelId, rateLimitSettings);
        }
    }

    public static Ai21ChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;

        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, context).build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    private final String modelId;
    private final RateLimitSettings rateLimitSettings;

    public Ai21ChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    public Ai21ChatCompletionServiceSettings(String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = modelId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ML_INFERENCE_AI21_COMPLETION_ADDED;
    }

    @Override
    public String modelId() {
        return this.modelId;
    }

    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        this.toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID, this.modelId);

        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Ai21ChatCompletionServiceSettings that = (Ai21ChatCompletionServiceSettings) o;
        return Objects.equals(modelId, that.modelId) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, rateLimitSettings);
    }

    @Override
    public Ai21ChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse AI21 chat completion service settings update", e);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code rate_limit} field. Including any immutable field (such as
     * {@code model_id}) causes the strict parser to reject the request.
     */
    private static class Update {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            RateLimitSettings.declareUpdatableRateLimitSettings(PARSER, Update::setRateLimitSettings);
        }

        private StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();

        private void setRateLimitSettings(StatefulValue<RateLimitSettings> rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        public Ai21ChatCompletionServiceSettings mergeInto(Ai21ChatCompletionServiceSettings existing) {
            return new Ai21ChatCompletionServiceSettings(
                existing.modelId(),
                applyUpdate(rateLimitSettings, existing.rateLimitSettings(), DEFAULT_RATE_LIMIT_SETTINGS)
            );
        }
    }
}
