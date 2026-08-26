/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.completion;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;

/**
 * Represents the settings for an Amazon Bedrock chat completion service. Extends {@link AmazonBedrockChatCompletionServiceSettings}, which
 * carries the model ID, region, provider, and rate limit settings shared across all Bedrock tasks. Chat completion adds no settings of its
 * own.
 */
public class AmazonBedrockChatCompletionServiceSettings extends AmazonBedrockServiceSettings {
    public static final String NAME = "amazon_bedrock_chat_completion_service_settings";

    public static AmazonBedrockChatCompletionServiceSettings fromMap(
        Map<String, Object> serviceSettings,
        ConfigurationParseContext context
    ) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return AmazonBedrockServiceSettings.fromMap(serviceSettings, context, parser);
    }

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the Amazon Bedrock chat completion service settings.
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
        AmazonBedrockServiceSettings.declareCommonFields(parser);
        return parser;
    }

    /**
     * Builds a {@link AmazonBedrockChatCompletionServiceSettings} from the common Bedrock fields, enforcing that the required fields are
     * present.
     */
    public static class Builder extends AmazonBedrockServiceSettings.Builder<AmazonBedrockChatCompletionServiceSettings> {

        @Override
        protected AmazonBedrockChatCompletionServiceSettings build(
            String region,
            String model,
            AmazonBedrockProvider provider,
            RateLimitSettings rateLimitSettings,
            ConfigurationParseContext context
        ) {
            return new AmazonBedrockChatCompletionServiceSettings(region, model, provider, rateLimitSettings);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code rate_limit} field. Including any immutable field (such as
     * {@code model}, {@code region} or {@code provider}) causes the strict parser to reject the request.
     */
    private static class Update extends AmazonBedrockServiceSettings.CommonUpdate {
        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            AmazonBedrockServiceSettings.declareCommonUpdatableFields(PARSER);
        }

        public AmazonBedrockChatCompletionServiceSettings mergeInto(AmazonBedrockChatCompletionServiceSettings existing) {
            return new AmazonBedrockChatCompletionServiceSettings(
                existing.region(),
                existing.modelId(),
                existing.provider(),
                mergedRateLimitSettings(existing)
            );
        }
    }

    public AmazonBedrockChatCompletionServiceSettings(
        String region,
        String model,
        AmazonBedrockProvider provider,
        RateLimitSettings rateLimitSettings
    ) {
        super(region, model, provider, rateLimitSettings);
    }

    public AmazonBedrockChatCompletionServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        super.addBaseXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        super.addXContentFragmentOfExposedFields(builder, params);
        return builder;
    }

    @Override
    public AmazonBedrockChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse Amazon Bedrock chat completion service settings update", e);
        }
    }
}
