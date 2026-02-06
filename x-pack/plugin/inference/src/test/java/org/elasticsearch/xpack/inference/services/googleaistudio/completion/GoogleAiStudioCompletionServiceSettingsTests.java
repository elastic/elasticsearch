/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class GoogleAiStudioCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<GoogleAiStudioCompletionServiceSettings> {
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public static GoogleAiStudioCompletionServiceSettings createRandom() {
        return new GoogleAiStudioCompletionServiceSettings(randomAlphaOfLength(8), randomFrom(RateLimitSettingsTests.createRandom(), null));
    }

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );

        var serviceSettings = new GoogleAiStudioCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(settingsMap, TaskType.COMPLETION);

        MatcherAssert.assertThat(
            serviceSettings,
            is(new GoogleAiStudioCompletionServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = new GoogleAiStudioCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(new HashMap<>(), TaskType.COMPLETION);

        MatcherAssert.assertThat(
            serviceSettings,
            is(new GoogleAiStudioCompletionServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var serviceSettings = GoogleAiStudioCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new GoogleAiStudioCompletionServiceSettings(TEST_MODEL_ID, null)));
    }

    public void testToXContent_WritesAllValues_DefaultRateLimit() throws IOException {
        var entity = new GoogleAiStudioCompletionServiceSettings(TEST_MODEL_ID, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(String.format("""
            {"model_id":"%s","rate_limit":{"requests_per_minute":360}}""", TEST_MODEL_ID)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new GoogleAiStudioCompletionServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(String.format("""
            {"model_id":"%s","rate_limit":{"requests_per_minute":%d}}""", TEST_MODEL_ID, TEST_RATE_LIMIT)));
    }

    @Override
    protected Writeable.Reader<GoogleAiStudioCompletionServiceSettings> instanceReader() {
        return GoogleAiStudioCompletionServiceSettings::new;
    }

    @Override
    protected GoogleAiStudioCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleAiStudioCompletionServiceSettings mutateInstance(GoogleAiStudioCompletionServiceSettings instance) throws IOException {
        if (randomBoolean()) {
            var modelId = randomValueOtherThan(instance.modelId(), () -> randomAlphaOfLength(8));
            return new GoogleAiStudioCompletionServiceSettings(modelId, instance.rateLimitSettings());
        } else {
            var rateLimitSettings = randomValueOtherThan(instance.rateLimitSettings(), RateLimitSettingsTests::createRandom);
            return new GoogleAiStudioCompletionServiceSettings(instance.modelId(), rateLimitSettings);
        }
    }
}
