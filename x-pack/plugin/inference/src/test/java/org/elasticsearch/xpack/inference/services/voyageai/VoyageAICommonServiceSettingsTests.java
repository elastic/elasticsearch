/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

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
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class VoyageAICommonServiceSettingsTests extends AbstractBWCWireSerializationTestCase<VoyageAICommonServiceSettings> {

    public static final String TEST_MODEL_ID = "test-model-id";
    public static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    public static final int TEST_RATE_LIMIT = 20;
    public static final int INITIAL_TEST_RATE_LIMIT = 30;
    public static final int DEFAULT_RATE_LIMIT = 2_000;

    public static VoyageAICommonServiceSettings createRandom() {
        return new VoyageAICommonServiceSettings(randomAlphaOfLength(15), RateLimitSettingsTests.createRandom());
    }

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var validationException = new ValidationException();
        var serviceSettings = VoyageAICommonServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values()),
            validationException
        );

        assertThat(serviceSettings, is(new VoyageAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))));
        assertThat(validationException.validationErrors(), is(empty()));
    }

    public void testFromMap_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var validationException = new ValidationException();
        var serviceSettings = VoyageAICommonServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null),
            randomFrom(ConfigurationParseContext.values()),
            validationException
        );

        assertThat(serviceSettings, is(new VoyageAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT))));
        assertThat(validationException.validationErrors(), is(empty()));
    }

    public void testFromMap_NoModelId_AccumulatesValidationError() {
        var validationException = new ValidationException();
        var serviceSettings = VoyageAICommonServiceSettings.fromMap(
            buildServiceSettingsMap(null, null),
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
        var originalServiceSettings = new VoyageAICommonServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var validationException = new ValidationException();
        var updatedServiceSettings = originalServiceSettings.updateCommonServiceSettings(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            validationException
        );

        assertThat(validationException.validationErrors(), is(empty()));
        assertThat(
            updatedServiceSettings,
            is(new VoyageAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateCommonServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new VoyageAICommonServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var validationException = new ValidationException();
        var updatedServiceSettings = originalServiceSettings.updateCommonServiceSettings(new HashMap<>(), validationException);

        assertThat(validationException.validationErrors(), is(empty()));
        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new VoyageAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
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
    protected Writeable.Reader<VoyageAICommonServiceSettings> instanceReader() {
        return VoyageAICommonServiceSettings::new;
    }

    @Override
    protected VoyageAICommonServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected VoyageAICommonServiceSettings mutateInstance(VoyageAICommonServiceSettings instance) throws IOException {
        if (randomBoolean()) {
            var modelId = randomValueOtherThan(instance.modelId(), () -> randomAlphaOfLength(15));
            return new VoyageAICommonServiceSettings(modelId, instance.rateLimitSettings());
        } else {
            var rateLimitSettings = randomValueOtherThan(instance.rateLimitSettings(), RateLimitSettingsTests::createRandom);
            return new VoyageAICommonServiceSettings(instance.modelId(), rateLimitSettings);
        }
    }

    private static Map<String, Object> buildServiceSettingsMap(@Nullable String modelId, @Nullable Integer rateLimit) {
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
    protected VoyageAICommonServiceSettings mutateInstanceForVersion(VoyageAICommonServiceSettings instance, TransportVersion version) {
        return instance;
    }
}
