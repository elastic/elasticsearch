/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class TencentCloudRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<TencentCloudRerankServiceSettings> {

    public static TencentCloudRerankServiceSettings createRandom() {
        return new TencentCloudRerankServiceSettings(TencentCloudCommonServiceSettingsTests.createRandom());
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
        return new TencentCloudRerankServiceSettings(
            randomValueOtherThan(instance.getCommonSettings(), TencentCloudCommonServiceSettingsTests::createRandom)
        );
    }

    @Override
    protected TencentCloudRerankServiceSettings mutateInstanceForVersion(
        TencentCloudRerankServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
