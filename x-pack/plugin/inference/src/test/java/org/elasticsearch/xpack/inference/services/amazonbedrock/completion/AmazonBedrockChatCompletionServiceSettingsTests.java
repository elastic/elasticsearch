/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.PROVIDER_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.REGION_FIELD;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AmazonBedrockChatCompletionServiceSettings> {
    private static final String TEST_REGION = "test-region";
    private static final String INITIAL_TEST_REGION = "initial-test-region";
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final AmazonBedrockProvider TEST_PROVIDER = AmazonBedrockProvider.AMAZONTITAN;
    private static final AmazonBedrockProvider INITIAL_TEST_PROVIDER = AmazonBedrockProvider.AI21LABS;
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public void testUpdateServiceSettings_AllFields_Success() {
        var newSettingsMap = createChatCompletionRequestSettingsMap(TEST_REGION, TEST_MODEL_ID, TEST_PROVIDER.toString(), TEST_RATE_LIMIT);
        var serviceSettings = new AmazonBedrockChatCompletionServiceSettings(
            INITIAL_TEST_REGION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROVIDER,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(newSettingsMap, TaskType.CHAT_COMPLETION);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockChatCompletionServiceSettings(
                    TEST_REGION,
                    TEST_MODEL_ID,
                    TEST_PROVIDER,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = new AmazonBedrockChatCompletionServiceSettings(
            INITIAL_TEST_REGION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROVIDER,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(new HashMap<>(), TaskType.CHAT_COMPLETION);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockChatCompletionServiceSettings(
                    INITIAL_TEST_REGION,
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_PROVIDER,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                )
            )
        );
    }

    private static HashMap<String, Object> createChatCompletionRequestSettingsMap(
        String region,
        String modelId,
        String provider,
        int rateLimit
    ) {
        var newSettingsMap = createChatCompletionRequestSettingsMap(region, modelId, provider);
        newSettingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        return newSettingsMap;
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var serviceSettings = AmazonBedrockChatCompletionServiceSettings.fromMap(
            createChatCompletionRequestSettingsMap(TEST_REGION, TEST_MODEL_ID, TEST_PROVIDER.toString()),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new AmazonBedrockChatCompletionServiceSettings(TEST_REGION, TEST_MODEL_ID, TEST_PROVIDER, null)));
    }

    public void testFromMap_RequestWithRateLimit_CreatesSettingsCorrectly() {
        var settingsMap = createChatCompletionRequestSettingsMap(TEST_REGION, TEST_MODEL_ID, TEST_PROVIDER.toString(), TEST_RATE_LIMIT);

        var serviceSettings = AmazonBedrockChatCompletionServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockChatCompletionServiceSettings(
                    TEST_REGION,
                    TEST_MODEL_ID,
                    TEST_PROVIDER,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var settingsMap = createChatCompletionRequestSettingsMap(TEST_REGION, TEST_MODEL_ID, TEST_PROVIDER.toString());
        var serviceSettings = AmazonBedrockChatCompletionServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings, is(new AmazonBedrockChatCompletionServiceSettings(TEST_REGION, TEST_MODEL_ID, TEST_PROVIDER, null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new AmazonBedrockChatCompletionServiceSettings(
            TEST_REGION,
            TEST_MODEL_ID,
            TEST_PROVIDER,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                Strings.format(
                    """
                        {"region":"%s","model":"%s","provider":"%s","rate_limit":{"requests_per_minute":%s}}""",
                    TEST_REGION,
                    TEST_MODEL_ID,
                    TEST_PROVIDER.name(),
                    TEST_RATE_LIMIT
                )
            )
        );
    }

    public static HashMap<String, Object> createChatCompletionRequestSettingsMap(String region, String model, String provider) {
        return new HashMap<>(Map.of(REGION_FIELD, region, MODEL_FIELD, model, PROVIDER_FIELD, provider));
    }

    @Override
    protected AmazonBedrockChatCompletionServiceSettings mutateInstanceForVersion(
        AmazonBedrockChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AmazonBedrockChatCompletionServiceSettings> instanceReader() {
        return AmazonBedrockChatCompletionServiceSettings::new;
    }

    @Override
    protected AmazonBedrockChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AmazonBedrockChatCompletionServiceSettings mutateInstance(AmazonBedrockChatCompletionServiceSettings instance)
        throws IOException {
        var region = instance.region();
        var modelId = instance.modelId();
        var provider = instance.provider();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(3)) {
            case 0 -> region = randomValueOtherThan(region, () -> randomAlphaOfLength(10));
            case 1 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(10));
            case 2 -> provider = randomValueOtherThan(provider, () -> randomFrom(AmazonBedrockProvider.values()));
            case 3 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AmazonBedrockChatCompletionServiceSettings(region, modelId, provider, rateLimitSettings);
    }

    private static AmazonBedrockChatCompletionServiceSettings createRandom() {
        return new AmazonBedrockChatCompletionServiceSettings(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomFrom(AmazonBedrockProvider.values()),
            RateLimitSettingsTests.createRandom()
        );
    }
}
