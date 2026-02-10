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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS;
import static org.elasticsearch.xpack.inference.services.settings.RateLimitSettings.REQUESTS_PER_MINUTE_FIELD;
import static org.hamcrest.Matchers.is;

public class JinaAIServiceSettingsTests extends AbstractBWCWireSerializationTestCase<JinaAIServiceSettings> {
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public static JinaAIServiceSettings createRandom() {
        var model = randomAlphaOfLength(15);

        return new JinaAIServiceSettings(model, RateLimitSettingsTests.createRandom());
    }

    public void testUpdateServiceSettings_AllFields_Success() {
        Map<String, Object> settingsMap = getServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT);

        var serviceSettings = new JinaAIServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT))
            .updateServiceSettings(settingsMap, TaskType.COMPLETION);

        assertThat(serviceSettings, is(new JinaAIServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = new JinaAIServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT))
            .updateServiceSettings(new HashMap<>(), TaskType.COMPLETION);

        assertThat(serviceSettings, is(new JinaAIServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT))));
    }

    public void testFromMap_DefaultRateLimitSettings() {
        var serviceSettings = JinaAIServiceSettings.fromMap(getServiceSettingsMap(TEST_MODEL_ID, null), ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings, is(new JinaAIServiceSettings(TEST_MODEL_ID, DEFAULT_RATE_LIMIT_SETTINGS)));
    }

    public void testFromMap_WithRateLimit() {
        var serviceSettings = JinaAIServiceSettings.fromMap(
            getServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new JinaAIServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testFromMap_WhenUsingModelId() {
        var serviceSettings = JinaAIServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new JinaAIServiceSettings(TEST_MODEL_ID, null)));
    }

    public void testXContent_WritesAllFields() throws IOException {
        var entity = new JinaAIServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","rate_limit":{"requests_per_minute":%d}}""", TEST_MODEL_ID, TEST_RATE_LIMIT)));
    }

    @Override
    protected Writeable.Reader<JinaAIServiceSettings> instanceReader() {
        return JinaAIServiceSettings::new;
    }

    @Override
    protected JinaAIServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected JinaAIServiceSettings mutateInstance(JinaAIServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(1)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(15));
            case 1 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new JinaAIServiceSettings(modelId, rateLimitSettings);
    }

    public static Map<String, Object> getServiceSettingsMap(String model, @Nullable Integer requestsPerMinute) {
        var map = new HashMap<String, Object>();

        map.put(ServiceFields.MODEL_ID, model);

        if (requestsPerMinute != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(REQUESTS_PER_MINUTE_FIELD, requestsPerMinute)));
        }

        return map;
    }

    @Override
    protected JinaAIServiceSettings mutateInstanceForVersion(JinaAIServiceSettings instance, TransportVersion version) {
        return instance;
    }
}
