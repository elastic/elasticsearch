/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.settings.RateLimitSettings.REQUESTS_PER_MINUTE_FIELD;

public class MixedbreadRerankServiceSettingsTests extends AbstractWireSerializingTestCase<MixedbreadRerankServiceSettings> {
    private static final String MODEL = "model";
    private static final RateLimitSettings RATE_LIMIT = new RateLimitSettings(2);

    public static MixedbreadRerankServiceSettings createRandom() {
        return createRandom(randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() }));
    }

    public static MixedbreadRerankServiceSettings createRandom(@Nullable RateLimitSettings rateLimitSettings) {
        return new MixedbreadRerankServiceSettings(randomAlphaOfLength(10), rateLimitSettings);
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new MixedbreadRerankServiceSettings(MODEL, RATE_LIMIT);
        assertThat(getXContentResult(serviceSettings), equalToIgnoringWhitespaceInJsonString("""
            {
                "model_id":"model",
                "rate_limit": {
                    "requests_per_minute": 2
                }
            }
            """));
    }

    public void testToXContent_DoesNotWriteOptionalValues_DefaultRateLimit() throws IOException {
        var serviceSettings = new MixedbreadRerankServiceSettings(MODEL, null);
        assertThat(getXContentResult(serviceSettings), equalToIgnoringWhitespaceInJsonString("""
            {
                "model_id":"model",
                "rate_limit": {
                    "requests_per_minute": 100
                }
            }
            """));
    }

    private String getXContentResult(MixedbreadRerankServiceSettings serviceSettings) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        return Strings.toString(builder);
    }

    @Override
    protected Writeable.Reader<MixedbreadRerankServiceSettings> instanceReader() {
        return MixedbreadRerankServiceSettings::new;
    }

    @Override
    protected MixedbreadRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected MixedbreadRerankServiceSettings mutateInstance(MixedbreadRerankServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(1)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new MixedbreadRerankServiceSettings(modelId, rateLimitSettings);
    }

    public static Map<String, Object> getServiceSettingsMap(String model) {
        return getServiceSettingsMap(model, null);
    }

    public static Map<String, Object> getServiceSettingsMap(String model, @Nullable Integer requestsPerMinute) {
        var map = new HashMap<String, Object>();

        map.put(ServiceFields.MODEL_ID, model);

        if (requestsPerMinute != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(REQUESTS_PER_MINUTE_FIELD, requestsPerMinute)));
        }

        return map;
    }
}
