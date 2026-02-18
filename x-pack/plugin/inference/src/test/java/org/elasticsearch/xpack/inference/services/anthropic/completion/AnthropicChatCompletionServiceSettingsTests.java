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
import org.elasticsearch.inference.TaskType;
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
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public void testUpdateServiceSettings_AllFields_Success() {
        Map<String, Object> newSettingsMap = new HashMap<>(
            Map.of(
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );
        var serviceSettings = new AnthropicChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(newSettingsMap, TaskType.CHAT_COMPLETION);

        assertThat(serviceSettings, is(new AnthropicChatCompletionServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = new AnthropicChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(new HashMap<>(), TaskType.CHAT_COMPLETION);

        assertThat(
            serviceSettings,
            is(new AnthropicChatCompletionServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var serviceSettings = AnthropicChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new AnthropicChatCompletionServiceSettings(TEST_MODEL_ID, null)));
    }

    public void testFromMap_Request_CreatesSettingsCorrectly_WithRateLimit() {
        var serviceSettings = AnthropicChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    TEST_MODEL_ID,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new AnthropicChatCompletionServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new AnthropicChatCompletionServiceSettings(TEST_MODEL_ID, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","rate_limit":{"requests_per_minute":50}}""", TEST_MODEL_ID)));
    }

    public void testToXContent_WritesAllValues_WithCustomRateLimit() throws IOException {
        var serviceSettings = new AnthropicChatCompletionServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","rate_limit":{"requests_per_minute":%d}}""", TEST_MODEL_ID, TEST_RATE_LIMIT)));
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
        if (randomBoolean()) {
            return new AnthropicChatCompletionServiceSettings(
                randomValueOtherThan(instance.modelId(), () -> randomAlphaOfLength(8)),
                instance.rateLimitSettings()
            );
        } else {
            return new AnthropicChatCompletionServiceSettings(
                instance.modelId(),
                randomValueOtherThan(instance.rateLimitSettings(), RateLimitSettingsTests::createRandom)
            );
        }
    }

    private static AnthropicChatCompletionServiceSettings createRandom() {
        return new AnthropicChatCompletionServiceSettings(randomAlphaOfLength(8), RateLimitSettingsTests.createRandom());
    }

    @Override
    protected AnthropicChatCompletionServiceSettings mutateInstanceForVersion(
        AnthropicChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
