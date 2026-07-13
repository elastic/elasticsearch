/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

public class Ai21ChatCompletionServiceSettingsTests extends AbstractBWCSerializationTestCase<Ai21ChatCompletionServiceSettings> {

    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public void testUpdateServiceSettings_RateLimit_IsUpdated() {
        var originalServiceSettings = new Ai21ChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)))
        );

        assertThat(
            updatedServiceSettings,
            is(new Ai21ChatCompletionServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new Ai21ChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_EmptyRateLimitObject_UsesDefaultValue() {
        var originalServiceSettings = new Ai21ChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>()))
        );

        assertThat(
            updatedServiceSettings,
            is(
                new Ai21ChatCompletionServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    new RateLimitSettings(Ai21ChatCompletionServiceSettings.DEFAULT_REQUESTS_PER_MINUTE)
                )
            )
        );
    }

    public void testUpdateServiceSettings_GivenImmutableFields_ThrowsException() {
        var serviceSettings = new Ai21ChatCompletionServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));

        for (String immutableField : List.of(ServiceFields.MODEL_ID)) {
            var e = expectThrows(
                XContentParseException.class,
                () -> serviceSettings.updateServiceSettings(new HashMap<>(Map.of(immutableField, "value")))
            );
            assertThat(
                e.getMessage(),
                endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, immutableField))
            );
        }
    }

    public void testFromMap_AllFields_Success() {
        var serviceSettings = Ai21ChatCompletionServiceSettings.fromMap(
            getServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new Ai21ChatCompletionServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testFromMap_MissingModelId_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> Ai21ChatCompletionServiceSettings.fromMap(
                getServiceSettingsMap(null, TEST_RATE_LIMIT),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "[%s] does not contain the required setting [%s]",
                    ModelConfigurations.SERVICE_SETTINGS,
                    ServiceFields.MODEL_ID
                )
            )
        );
    }

    public void testFromMap_MissingRateLimit_Success() {
        var serviceSettings = Ai21ChatCompletionServiceSettings.fromMap(
            getServiceSettingsMap(TEST_MODEL_ID, null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new Ai21ChatCompletionServiceSettings(TEST_MODEL_ID, new RateLimitSettings(200))));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = Ai21ChatCompletionServiceSettings.fromMap(
            getServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            ConfigurationParseContext.PERSISTENT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_RATE_LIMIT));

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_DoesNotWriteOptionalValues_DefaultRateLimit() throws IOException {
        var serviceSettings = Ai21ChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID)),
            ConfigurationParseContext.PERSISTENT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, 200));
        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<Ai21ChatCompletionServiceSettings> instanceReader() {
        return Ai21ChatCompletionServiceSettings::new;
    }

    @Override
    protected Ai21ChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected Ai21ChatCompletionServiceSettings mutateInstance(Ai21ChatCompletionServiceSettings instance) throws IOException {
        if (randomBoolean()) {
            return new Ai21ChatCompletionServiceSettings(
                randomValueOtherThan(instance.modelId(), () -> randomAlphaOfLength(8)),
                instance.rateLimitSettings()
            );
        } else {
            return new Ai21ChatCompletionServiceSettings(
                instance.modelId(),
                randomValueOtherThan(instance.rateLimitSettings(), RateLimitSettingsTests::createRandom)
            );
        }
    }

    @Override
    protected Ai21ChatCompletionServiceSettings mutateInstanceForVersion(
        Ai21ChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static Ai21ChatCompletionServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);

        return new Ai21ChatCompletionServiceSettings(modelId, RateLimitSettingsTests.createRandom());
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String modelId, @Nullable Integer rateLimit) {
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
    protected Ai21ChatCompletionServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return Ai21ChatCompletionServiceSettings.createParser(true).apply(parser, ConfigurationParseContext.PERSISTENT).build();
    }
}
