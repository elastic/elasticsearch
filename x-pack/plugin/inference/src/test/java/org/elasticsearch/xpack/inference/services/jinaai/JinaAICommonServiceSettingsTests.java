/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
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

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class JinaAICommonServiceSettingsTests extends AbstractBWCWireSerializationTestCase<JinaAICommonServiceSettings> {

    private static final String TEST_MODEL_ID = "test-model";
    private static final String INITIAL_TEST_MODEL_ID = "initial-model";

    private static final int TEST_RATE_LIMIT = 500;
    private static final int INITIAL_TEST_RATE_LIMIT = 100;
    private static final int DEFAULT_RATE_LIMIT = 2000;

    public static JinaAICommonServiceSettings createRandom() {
        var model = randomAlphaOfLength(15);

        return new JinaAICommonServiceSettings(model, RateLimitSettingsTests.createRandom());
    }

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var settingsMap = buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT);

        var serviceSettings = JinaAICommonServiceSettings.fromMap(
            settingsMap,
            randomFrom(ConfigurationParseContext.values()),
            new ValidationException()
        );

        assertThat(serviceSettings, is(new JinaAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testFromMap_OnlyMandatoryFields_UsesDefaultRateLimit_Success() {
        var settingsMap = buildServiceSettingsMap(TEST_MODEL_ID, null);

        var serviceSettings = JinaAICommonServiceSettings.fromMap(
            settingsMap,
            randomFrom(ConfigurationParseContext.values()),
            new ValidationException()
        );

        assertThat(serviceSettings, is(new JinaAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT))));
    }

    public void testFromMap_NoModelId_ThrowsValidationError() {
        var settingsMap = buildServiceSettingsMap(null, TEST_RATE_LIMIT);

        var validationException = new ValidationException();
        var serviceSettings = JinaAICommonServiceSettings.fromMap(
            settingsMap,
            randomFrom(ConfigurationParseContext.values()),
            validationException
        );

        assertThat(serviceSettings, is(nullValue()));
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(
            validationException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.MODEL_ID))
        );
    }

    public void testUpdateCommonServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT);
        var originalServiceSettings = new JinaAICommonServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var validationException = new ValidationException();

        var updatedServiceSettings = originalServiceSettings.updateCommonServiceSettings(settingsMap, validationException);

        assertThat(
            updatedServiceSettings,
            is(new JinaAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
        assertThat(validationException.validationErrors(), is(empty()));
    }

    public void testUpdateCommonServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new JinaAICommonServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var validationException = new ValidationException();

        var serviceSettings = originalServiceSettings.updateCommonServiceSettings(new HashMap<>(), validationException);

        assertThat(serviceSettings, is(originalServiceSettings));
        assertThat(validationException.validationErrors(), is(empty()));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new JinaAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT));

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
