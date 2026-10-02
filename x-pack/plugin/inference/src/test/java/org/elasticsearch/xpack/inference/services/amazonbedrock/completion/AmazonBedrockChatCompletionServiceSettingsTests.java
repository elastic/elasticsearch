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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AbstractAmazonBedrockServiceSettingsTests;
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

public class AmazonBedrockChatCompletionServiceSettingsTests extends AbstractAmazonBedrockServiceSettingsTests<
    AmazonBedrockChatCompletionServiceSettings> {

    @Override
    protected AmazonBedrockChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AmazonBedrockChatCompletionServiceSettings.fromMap(map, context);
    }

    @Override
    protected Map<String, Object> buildCommonServiceSettingsMap(String region, String model, String provider, Integer rateLimit) {
        return buildServiceSettingsMap(region, model, provider, rateLimit);
    }

    @Override
    protected AmazonBedrockChatCompletionServiceSettings createServiceSettings(
        String region,
        String model,
        AmazonBedrockProvider provider,
        RateLimitSettings rateLimitSettings
    ) {
        return new AmazonBedrockChatCompletionServiceSettings(region, model, provider, rateLimitSettings);
    }

    @Override
    protected AmazonBedrockChatCompletionServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return AmazonBedrockChatCompletionServiceSettings.createParser(true)
            .apply(parser, ConfigurationParseContext.PERSISTENT)
            .build(ConfigurationParseContext.PERSISTENT);
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

    public static Map<String, Object> buildServiceSettingsMap(
        @Nullable String region,
        @Nullable String model,
        @Nullable String provider,
        @Nullable Integer rateLimit
    ) {
        var map = new HashMap<String, Object>();
        if (region != null) {
            map.put(REGION_FIELD, region);
        }
        if (model != null) {
            map.put(MODEL_FIELD, model);
        }
        if (provider != null) {
            map.put(PROVIDER_FIELD, provider);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }

}
