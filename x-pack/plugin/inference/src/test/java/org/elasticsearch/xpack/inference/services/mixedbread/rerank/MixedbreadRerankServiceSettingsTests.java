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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class MixedbreadRerankServiceSettingsTests extends AbstractWireSerializingTestCase<MixedbreadRerankServiceSettings> {
    private static final String MODEL = "model";
    private static final RateLimitSettings RATE_LIMIT = new RateLimitSettings(2);
    private static final Integer WINDOWS_SIZE = 512;
    private static final URI URI = java.net.URI.create("uri");

    public static MixedbreadRerankServiceSettings createRandom() {
        return createRandom(randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() }));
    }

    public static MixedbreadRerankServiceSettings createRandom(@Nullable RateLimitSettings rateLimitSettings) {
        return new MixedbreadRerankServiceSettings(randomAlphaOfLengthOrNull(10), rateLimitSettings, null, null);
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new MixedbreadRerankServiceSettings(MODEL, RATE_LIMIT, URI, WINDOWS_SIZE);
        assertThat(getXContentResult(serviceSettings), equalToIgnoringWhitespaceInJsonString("""
            {
                "model_id":"model",
                "rate_limit": {
                    "requests_per_minute": 2
                },
                "url": "uri",
                "windows_size": 512
            }
            """));
    }


    public void testToXContent_DoesNotWriteOptionalValues_DefaultRateLimit_And_DefaultWindowsSize() throws IOException {
        var serviceSettings = new MixedbreadRerankServiceSettings(MODEL, null, null, null);
        assertThat(getXContentResult(serviceSettings), equalToIgnoringWhitespaceInJsonString("""
            {
                "model_id":"model",
                "rate_limit": {
                    "requests_per_minute": 240
                },
                "windows_size": 8000
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
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(10));
            case 1 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new MixedbreadRerankServiceSettings(modelId, rateLimitSettings, null, null);
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model) {
        var map = new HashMap<String, Object>();

        if (url != null) {
            map.put(ServiceFields.URL, url);
        }

        if (model != null) {
            map.put(ServiceFields.MODEL_ID, model);
        }

        return map;
    }
}
