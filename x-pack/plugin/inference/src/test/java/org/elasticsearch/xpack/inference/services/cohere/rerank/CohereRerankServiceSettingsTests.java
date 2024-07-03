/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class CohereRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<CohereRerankServiceSettings> {
    public static CohereRerankServiceSettings createRandom() {
        return createRandom(randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() }));
    }

    public static CohereRerankServiceSettings createRandom(@Nullable RateLimitSettings rateLimitSettings) {
        return new CohereRerankServiceSettings(
            randomFrom(new String[] { null, Strings.format("http://%s.com", randomAlphaOfLength(8)) }),
            randomFrom(new String[] { null, randomAlphaOfLength(10) }),
            rateLimitSettings
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var url = "http://www.abc.com";
        var model = "model";

        var serviceSettings = new CohereRerankServiceSettings(url, model, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "url":"http://www.abc.com",
                "model_id":"model",
                "rate_limit": {
                    "requests_per_minute": 10000
                }
            }
            """));
    }

    @Override
    protected Writeable.Reader<CohereRerankServiceSettings> instanceReader() {
        return CohereRerankServiceSettings::new;
    }

    @Override
    protected CohereRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereRerankServiceSettings mutateInstance(CohereRerankServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, CohereRerankServiceSettingsTests::createRandom);
    }

    @Override
    protected CohereRerankServiceSettings mutateInstanceForVersion(CohereRerankServiceSettings instance, TransportVersion version) {
        if (version.before(TransportVersions.ML_INFERENCE_RATE_LIMIT_SETTINGS_ADDED)) {
            // We always default to the same rate limit settings, if a node is on a version before rate limits were introduced
            return new CohereRerankServiceSettings(instance.uri(), instance.modelId(), CohereServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS);
        }
        return instance;
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model) {
        return new HashMap<>(CohereServiceSettingsTests.getServiceSettingsMap(url, model));
    }
}
