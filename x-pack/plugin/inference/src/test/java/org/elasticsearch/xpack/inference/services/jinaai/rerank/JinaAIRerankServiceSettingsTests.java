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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS;
import static org.hamcrest.Matchers.is;

public class JinaAIRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<JinaAIRerankServiceSettings> {
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public static JinaAIRerankServiceSettings createRandom() {
        return new JinaAIRerankServiceSettings(new JinaAIServiceSettings(randomAlphaOfLength(10), RateLimitSettingsTests.createRandom()));
    }

    public void testUpdateServiceSettings_AllFields_Success() {
        Map<String, Object> settingsMap = getServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT);

        var serviceSettings = new JinaAIRerankServiceSettings(
            new JinaAIServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT))
        ).updateServiceSettings(
            settingsMap,
            TaskType.RERANK

        );

        assertThat(
            serviceSettings,
            is(new JinaAIRerankServiceSettings(new JinaAIServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = new JinaAIRerankServiceSettings(
            new JinaAIServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT))
        ).updateServiceSettings(
            new HashMap<>(),
            TaskType.RERANK

        );

        assertThat(
            serviceSettings,
            is(
                new JinaAIRerankServiceSettings(
                    new JinaAIServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT))
                )
            )
        );
    }

    public void testFromMap_DefaultRateLimitSettings() {
        var serviceSettings = JinaAIRerankServiceSettings.fromMap(
            getServiceSettingsMap(TEST_MODEL_ID, null),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(new JinaAIRerankServiceSettings(new JinaAIServiceSettings(TEST_MODEL_ID, DEFAULT_RATE_LIMIT_SETTINGS)))
        );
    }

    public void testFromMap_WithRateLimit() {
        var serviceSettings = JinaAIRerankServiceSettings.fromMap(
            getServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(new JinaAIRerankServiceSettings(new JinaAIServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))))
        );
    }

    public void testFromMap_WhenUsingModelId() {
        var serviceSettings = JinaAIRerankServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new JinaAIRerankServiceSettings(new JinaAIServiceSettings(TEST_MODEL_ID, null))));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new JinaAIRerankServiceSettings(
            new JinaAIServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "model_id":"%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_RATE_LIMIT)));
    }

    public void testToXContent_WritesAllValues_DefaultRateLimit() throws IOException {
        var serviceSettings = new JinaAIRerankServiceSettings(new JinaAIServiceSettings(TEST_MODEL_ID, null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "model_id":"%s",
                "rate_limit": {
                    "requests_per_minute": 2000
                }
            }
            """, TEST_MODEL_ID)));
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
        JinaAIServiceSettings commonSettings = randomValueOtherThan(instance.getCommonSettings(), JinaAIServiceSettingsTests::createRandom);
        return new JinaAIRerankServiceSettings(commonSettings);
    }

    @Override
    protected JinaAIRerankServiceSettings mutateInstanceForVersion(JinaAIRerankServiceSettings instance, TransportVersion version) {
        return instance;
    }

    public static Map<String, Object> getServiceSettingsMap(String model) {
        return getServiceSettingsMap(model, null);
    }

    public static Map<String, Object> getServiceSettingsMap(String model, @Nullable Integer requestsPerMinute) {
        return JinaAIServiceSettingsTests.getServiceSettingsMap(model, requestsPerMinute);
    }
}
