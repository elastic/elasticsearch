/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.rerank;

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

public class TencentCloudRerankServiceSettingsTests extends AbstractTencentCloudServiceSettingsTests<TencentCloudRerankServiceSettings> {

    public static TencentCloudRerankServiceSettings createRandom() {
        return new TencentCloudRerankServiceSettings(
            randomAlphaOfLength(8),
            randomBoolean() ? randomAlphaOfLength(5) : null,
            new RateLimitSettings(randomIntBetween(1, 1000))
        );
    }

    @Override
    protected TencentCloudCommonServiceSettings createInstance(String modelId, String region, RateLimitSettings rateLimitSettings) {
        return new TencentCloudRerankServiceSettings(modelId, region, rateLimitSettings);
    }

    public void testFromMap_MinimalConfig() {
        var settings = TencentCloudRerankServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, "bge-reranker-v2-m3")),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings.modelId(), is("bge-reranker-v2-m3"));
    }

    public void testFromMap_WithRateLimit() {
        var settings = TencentCloudRerankServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    "bge-reranker-large",
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 30))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(30)));
    }

    @Override
    protected Writeable.Reader<TencentCloudRerankServiceSettings> instanceReader() {
        return TencentCloudRerankServiceSettings::new;
    }

    @Override
    protected TencentCloudRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected TencentCloudRerankServiceSettings mutateInstance(TencentCloudRerankServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var region = instance.region();
        var rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> region = randomValueOtherThan(region, () -> randomAlphaOfLength(5));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, () -> new RateLimitSettings(randomIntBetween(1, 1000)));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new TencentCloudRerankServiceSettings(modelId, region, rateLimitSettings);
    }

    @Override
    protected TencentCloudRerankServiceSettings mutateInstanceForVersion(
        TencentCloudRerankServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
