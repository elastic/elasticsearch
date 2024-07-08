/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class AmazonBedrockChatCompletionServiceSettings extends AmazonBedrockServiceSettings {
    public static final String NAME = "amazon_bedrock_chat_completion_service_settings";

    public static AmazonBedrockChatCompletionServiceSettings fromMap(
        Map<String, Object> serviceSettings,
        ConfigurationParseContext context
    ) {
        ValidationException validationException = new ValidationException();

        var baseSettings = AmazonBedrockServiceSettings.fromMap(serviceSettings, validationException, context);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AmazonBedrockChatCompletionServiceSettings(
            baseSettings.region(),
            baseSettings.model(),
            baseSettings.provider(),
            baseSettings.rateLimitSettings()
        );
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AmazonBedrockChatCompletionServiceSettings that = (AmazonBedrockChatCompletionServiceSettings) o;

        return Objects.equals(region, that.region)
            && Objects.equals(provider, that.provider)
            && Objects.equals(model, that.model)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(region, model, provider, rateLimitSettings);
    }
}
