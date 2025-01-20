/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AnthropicChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AnthropicChatCompletionServiceSettings> {

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var modelId = "some model";

        var serviceSettings = AnthropicChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new AnthropicChatCompletionServiceSettings(modelId, null)));
    }

    public void testFromMap_Request_CreatesSettingsCorrectly_WithRateLimit() {
        var modelId = "some model";
        var rateLimit = 2;
        var serviceSettings = AnthropicChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new AnthropicChatCompletionServiceSettings(modelId, new RateLimitSettings(2))));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new AnthropicChatCompletionServiceSettings("model", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"model_id":"model","rate_limit":{"requests_per_minute":50}}"""));
    }

    public void testToXContent_WritesAllValues_WithCustomRateLimit() throws IOException {
        var serviceSettings = new AnthropicChatCompletionServiceSettings("model", new RateLimitSettings(2));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"model_id":"model","rate_limit":{"requests_per_minute":2}}"""));
    }

    @Override
    protected Writeable.Reader<AnthropicChatCompletionServiceSettings> instanceReader() {
        return AnthropicChatCompletionServiceSettings::new;
    }

    @Override
    protected AnthropicChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AnthropicChatCompletionServiceSettings mutateInstance(AnthropicChatCompletionServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, AnthropicChatCompletionServiceSettingsTests::createRandom);
    }

    private static AnthropicChatCompletionServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);

        return new AnthropicChatCompletionServiceSettings(modelId, RateLimitSettingsTests.createRandom());
    }

    @Override
    protected AnthropicChatCompletionServiceSettings mutateInstanceForVersion(
        AnthropicChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
