/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class JinaAICommonServiceSettingsTests extends AbstractBWCWireSerializationTestCase<JinaAICommonServiceSettings> {

    private static final String TEST_MODEL_ID = "test-model";

    private static final int TEST_RATE_LIMIT = 500;

    public static JinaAICommonServiceSettings createRandom() {
        var model = randomAlphaOfLength(15);

        return new JinaAICommonServiceSettings(model, RateLimitSettingsTests.createRandom());
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new JinaAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT));

        // The common settings render as an XContent fragment that is embedded in the owning service settings object, so wrap it in an
        // object here to produce a complete document for comparison.
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        serviceSettings.toXContent(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_RATE_LIMIT)));
    }

    @Override
    protected Writeable.Reader<JinaAICommonServiceSettings> instanceReader() {
        return JinaAICommonServiceSettings::new;
    }

    @Override
    protected JinaAICommonServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected JinaAICommonServiceSettings mutateInstance(JinaAICommonServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(1)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(15));
            case 1 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new JinaAICommonServiceSettings(modelId, rateLimitSettings);
    }

    public static Map<String, Object> buildServiceSettingsMap(@Nullable String modelId, @Nullable Integer rateLimit) {
        var map = new HashMap<String, Object>();

        if (modelId != null) {
            map.put(ServiceFields.MODEL_ID, modelId);
        }

        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }

        return map;
    }

    @Override
    protected JinaAICommonServiceSettings mutateInstanceForVersion(JinaAICommonServiceSettings instance, TransportVersion version) {
        return instance;
    }
}
