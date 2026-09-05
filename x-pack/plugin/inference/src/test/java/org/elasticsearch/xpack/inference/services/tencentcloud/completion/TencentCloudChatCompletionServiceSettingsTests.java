/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.AbstractTencentCloudServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class TencentCloudChatCompletionServiceSettingsTests extends AbstractTencentCloudServiceSettingsTests<
    TencentCloudChatCompletionServiceSettings> {

    public static TencentCloudChatCompletionServiceSettings createRandom() {
        return new TencentCloudChatCompletionServiceSettings(
            randomAlphaOfLength(8),
            randomBoolean() ? randomAlphaOfLength(5) : null,
            new RateLimitSettings(randomIntBetween(1, 1000))
        );
    }

    @Override
    protected TencentCloudCommonServiceSettings createInstance(String modelId, String region, RateLimitSettings rateLimitSettings) {
        return new TencentCloudChatCompletionServiceSettings(modelId, region, rateLimitSettings);
    }

    public void testFromMap_MinimalConfig_UsesChatCompletionDefaultRateLimit() {
        var settings = TencentCloudChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, "deepseek-v3")),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings.modelId(), is("deepseek-v3"));
        // Default should be the chat completion specific default (5 rpm), not the common default (20 rpm).
        assertThat(settings.rateLimitSettings(), is(TencentCloudChatCompletionServiceSettings.DEFAULT_CHAT_COMPLETION_RATE_LIMIT));
    }

    public void testFromMap_ExplicitRateLimit_Respected() {
        var settings = TencentCloudChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    "deepseek-v3",
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 42))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(42)));
    }

    @Override
    protected Writeable.Reader<TencentCloudChatCompletionServiceSettings> instanceReader() {
        return TencentCloudChatCompletionServiceSettings::new;
    }

    @Override
    protected TencentCloudChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected TencentCloudChatCompletionServiceSettings mutateInstance(TencentCloudChatCompletionServiceSettings instance)
        throws IOException {
        var modelId = instance.modelId();
        var region = instance.region();
        var rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> region = randomValueOtherThan(region, () -> randomAlphaOfLength(5));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, () -> new RateLimitSettings(randomIntBetween(1, 1000)));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new TencentCloudChatCompletionServiceSettings(modelId, region, rateLimitSettings);
    }

    @Override
    protected TencentCloudChatCompletionServiceSettings mutateInstanceForVersion(
        TencentCloudChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
