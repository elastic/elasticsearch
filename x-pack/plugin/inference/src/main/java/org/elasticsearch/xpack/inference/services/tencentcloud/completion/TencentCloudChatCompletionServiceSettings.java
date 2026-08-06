/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;

import java.io.IOException;
import java.util.Map;

/**
 * Settings for the TencentCloud chat completion / completion service. Holds only the fields common to every TencentCloud
 * task (model id, region, rate limit). The chat completion default rate limit (5 rpm) differs from the general default.
 */
public class TencentCloudChatCompletionServiceSettings extends TencentCloudCommonServiceSettings {

    public static final String NAME = "tencentcloud_chat_completion_service_settings";
    // Chat completion default rate limit is 5 rpm per the AI Gateway docs.
    public static final RateLimitSettings DEFAULT_CHAT_COMPLETION_RATE_LIMIT = new RateLimitSettings(5);

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            ignoreUnknownFields,
            Builder::new
        );
        TencentCloudCommonServiceSettings.declareCommonFields(parser, DEFAULT_CHAT_COMPLETION_RATE_LIMIT);
        return parser;
    }

    public static TencentCloudChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        var validationException = new ValidationException();
        var settings = TencentCloudCommonServiceSettings.fromMap(map, context, parser, validationException);
        validationException.throwIfValidationErrorsExist();
        return settings;
    }

    public TencentCloudChatCompletionServiceSettings(String modelId, String region, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, region, rateLimitSettings);
    }

    public TencentCloudChatCompletionServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TencentCloudChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings(),
            validationException,
            ConfigurationParseContext.REQUEST
        );
        validationException.throwIfValidationErrorsExist();
        return new TencentCloudChatCompletionServiceSettings(this.modelId(), this.region(), extractedRateLimitSettings);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    // ---- ObjectParser Builder ----

    public static class Builder extends TencentCloudCommonServiceSettings.Builder<TencentCloudChatCompletionServiceSettings> {
        @Override
        protected TencentCloudChatCompletionServiceSettings build() {
            // When the rate_limit field is absent, the builder's rateLimitSettings stays null. Apply the chat-completion-specific
            // default (5 rpm) here rather than the general default applied by the base constructor.
            var rateLimit = rateLimitSettings != null ? rateLimitSettings : DEFAULT_CHAT_COMPLETION_RATE_LIMIT;
            return new TencentCloudChatCompletionServiceSettings(modelId, region, rateLimit);
        }
    }
}
