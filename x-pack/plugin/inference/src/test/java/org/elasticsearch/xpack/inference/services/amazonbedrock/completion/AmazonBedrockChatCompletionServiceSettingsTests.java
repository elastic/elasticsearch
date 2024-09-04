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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.PROVIDER_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.REGION_FIELD;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AmazonBedrockChatCompletionServiceSettings> {

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";
        var serviceSettings = AmazonBedrockChatCompletionServiceSettings.fromMap(
            createChatCompletionRequestSettingsMap(region, model, provider),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(new AmazonBedrockChatCompletionServiceSettings(region, model, AmazonBedrockProvider.AMAZONTITAN, null))
        );
    }

    public void testFromMap_RequestWithRateLimit_CreatesSettingsCorrectly() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";
        var settingsMap = createChatCompletionRequestSettingsMap(region, model, provider);
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3)));

        var serviceSettings = AmazonBedrockChatCompletionServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(new AmazonBedrockChatCompletionServiceSettings(region, model, AmazonBedrockProvider.AMAZONTITAN, new RateLimitSettings(3)))
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";
        var settingsMap = createChatCompletionRequestSettingsMap(region, model, provider);
        var serviceSettings = AmazonBedrockChatCompletionServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(new AmazonBedrockChatCompletionServiceSettings(region, model, AmazonBedrockProvider.AMAZONTITAN, null))
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new AmazonBedrockChatCompletionServiceSettings(
            "testregion",
            "testmodel",
            AmazonBedrockProvider.AMAZONTITAN,
            new RateLimitSettings(3)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"region":"testregion","model":"testmodel","provider":"AMAZONTITAN",""" + """
            "rate_limit":{"requests_per_minute":3}}"""));
    }

    public static HashMap<String, Object> createChatCompletionRequestSettingsMap(String region, String model, String provider) {
        return new HashMap<String, Object>(Map.of(REGION_FIELD, region, MODEL_FIELD, model, PROVIDER_FIELD, provider));
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
        return randomValueOtherThan(instance, AmazonBedrockChatCompletionServiceSettingsTests::createRandom);
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
