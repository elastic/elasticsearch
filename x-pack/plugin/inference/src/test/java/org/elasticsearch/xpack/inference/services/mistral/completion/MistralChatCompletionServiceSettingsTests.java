/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.mistral.MistralConstants;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class MistralChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<MistralChatCompletionServiceSettings> {

    private static final String TEST_MODEL_ID = "mistral-small";
    private static final String INITIAL_TEST_MODEL_ID = "initial-mistral-small";

    private static final int TEST_RATE_LIMIT = 2;
    private static final int INITIAL_TEST_RATE_LIMIT = 100;

    private static final int DEFAULT_RATE_LIMIT = 240;

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = MistralChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(new MistralChatCompletionServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testFromMap_OnlyMandatoryFields_UsesDefaultValues_Success() {
        var serviceSettings = MistralChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(new MistralChatCompletionServiceSettings(TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT))));
    }

    public void testFromMap_NoModel_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> MistralChatCompletionServiceSettings.fromMap(
                buildServiceSettingsMap(null, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", MistralConstants.MODEL_FIELD))
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT);
        var originalServiceSettings = new MistralChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(new MistralChatCompletionServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new MistralChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = MistralChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "model": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_RATE_LIMIT));

        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<MistralChatCompletionServiceSettings> instanceReader() {
        return MistralChatCompletionServiceSettings::new;
    }

    @Override
    protected MistralChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected MistralChatCompletionServiceSettings mutateInstance(MistralChatCompletionServiceSettings instance) throws IOException {
        if (randomBoolean()) {
            var modelId = randomValueOtherThan(instance.modelId(), () -> randomAlphaOfLength(8));
            return new MistralChatCompletionServiceSettings(modelId, instance.rateLimitSettings());
        } else {
            var rateLimitSettings = randomValueOtherThan(instance.rateLimitSettings(), RateLimitSettingsTests::createRandom);
            return new MistralChatCompletionServiceSettings(instance.modelId(), rateLimitSettings);
        }
    }

    @Override
    protected MistralChatCompletionServiceSettings mutateInstanceForVersion(
        MistralChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static MistralChatCompletionServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);

        return new MistralChatCompletionServiceSettings(modelId, RateLimitSettingsTests.createRandom());
    }

    public static Map<String, Object> buildServiceSettingsMap(@Nullable String modelId, @Nullable Integer rateLimit) {
        var map = new HashMap<String, Object>();

        if (modelId != null) {
            map.put(MistralConstants.MODEL_FIELD, modelId);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }

        return map;
    }
}
