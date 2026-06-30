/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.AbstractJinaAIServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAICommonServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAICommonServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.hamcrest.Matchers.is;

public class JinaAIRerankServiceSettingsTests extends AbstractJinaAIServiceSettingsTests<JinaAIRerankServiceSettings> {

    public static JinaAIRerankServiceSettings createRandom() {
        return new JinaAIRerankServiceSettings(
            new JinaAICommonServiceSettings(randomAlphaOfLength(10), RateLimitSettingsTests.createRandom())
        );
    }

    @Override
    protected JinaAIRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return JinaAIRerankServiceSettings.fromMap(map, context);
    }

    @Override
    protected Map<String, Object> buildCommonServiceSettingsMap(@Nullable String modelId, @Nullable Integer rateLimit) {
        return JinaAICommonServiceSettingsTests.buildServiceSettingsMap(modelId, rateLimit);
    }

    @Override
    protected JinaAIRerankServiceSettings createServiceSettings(String modelId, RateLimitSettings rateLimitSettings) {
        return new JinaAIRerankServiceSettings(new JinaAICommonServiceSettings(modelId, rateLimitSettings));
    }

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var settingsMap = buildCommonServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT);

        var serviceSettings = JinaAIRerankServiceSettings.fromMap(settingsMap, randomFrom(ConfigurationParseContext.values()));

        assertThat(
            serviceSettings,
            is(new JinaAIRerankServiceSettings(new JinaAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))))
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new JinaAIRerankServiceSettings(
            new JinaAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
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
    protected Writeable.Reader<JinaAIRerankServiceSettings> instanceReader() {
        return JinaAIRerankServiceSettings::new;
    }

    @Override
    protected JinaAIRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected JinaAIRerankServiceSettings mutateInstance(JinaAIRerankServiceSettings instance) throws IOException {
        JinaAICommonServiceSettings commonSettings = randomValueOtherThan(
            instance.getCommonSettings(),
            JinaAICommonServiceSettingsTests::createRandom
        );
        return new JinaAIRerankServiceSettings(commonSettings);
    }

    @Override
    protected JinaAIRerankServiceSettings mutateInstanceForVersion(JinaAIRerankServiceSettings instance, TransportVersion version) {
        return instance;
    }

    public static Map<String, Object> buildServiceSettingsMap(@Nullable String modelId, @Nullable Integer rateLimit) {
        return JinaAICommonServiceSettingsTests.buildServiceSettingsMap(modelId, rateLimit);
    }
}
