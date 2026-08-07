/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.DEFAULT_RATE_LIMIT;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.INITIAL_TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.INITIAL_TEST_RATE_LIMIT;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.NEW_TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.NEW_TEST_RATE_LIMIT;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_RATE_LIMIT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ContextualAiRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<ContextualAiRerankServiceSettings> {

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = createRerankServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            buildServiceSettingsMap(NEW_TEST_MODEL_ID, NEW_TEST_RATE_LIMIT)
        );

        assertThat(
            updatedServiceSettings,
            is(createRerankServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(NEW_TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = createRerankServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testFromMap_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var serviceSettings = ContextualAiRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(createRerankServiceSettings(TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT))));
    }

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = ContextualAiRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(createRerankServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testFromMap_NoModelId_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> ContextualAiRerankServiceSettings.fromMap(
                buildServiceSettingsMap(null, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [model_id];")
        );
    }

    public void testFromMap_NoRateLimit_CreatesSettingsCorrectly() {
        var serviceSettings = ContextualAiRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(createRerankServiceSettings(TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT))));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var settings = createRerankServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_RATE_LIMIT))));
    }

    @Override
    protected Writeable.Reader<ContextualAiRerankServiceSettings> instanceReader() {
        return ContextualAiRerankServiceSettings::new;
    }

    @Override
    protected ContextualAiRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ContextualAiRerankServiceSettings mutateInstance(ContextualAiRerankServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();

        switch (randomInt(1)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return createRerankServiceSettings(modelId, rateLimitSettings);
    }

    @Override
    protected ContextualAiRerankServiceSettings mutateInstanceForVersion(
        ContextualAiRerankServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static ContextualAiRerankServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);
        return createRerankServiceSettings(modelId, RateLimitSettingsTests.createRandom());
    }

    private static ContextualAiRerankServiceSettings createRerankServiceSettings(String modelId, RateLimitSettings rateLimitSettings) {
        return new ContextualAiRerankServiceSettings(new ContextualAiServiceSettings.CommonSettings(modelId, rateLimitSettings));
    }

    public static HashMap<String, Object> buildServiceSettingsMap(@Nullable String modelId, @Nullable Integer rateLimit) {
        var result = new HashMap<String, Object>();
        if (modelId != null) {
            result.put(ServiceFields.MODEL_ID, modelId);
        }
        if (rateLimit != null) {
            result.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return result;
    }
}
