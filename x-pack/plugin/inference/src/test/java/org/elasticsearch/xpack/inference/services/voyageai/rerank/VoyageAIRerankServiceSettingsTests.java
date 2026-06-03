/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.rerank;

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

import static org.hamcrest.Matchers.is;

public class VoyageAIRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<VoyageAIRerankServiceSettings> {

    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;
    private static final int DEFAULT_RATE_LIMIT = 2_000;

    public static VoyageAIRerankServiceSettings createRandom() {
        return new VoyageAIRerankServiceSettings(randomAlphaOfLength(15), RateLimitSettingsTests.createRandom());
    }

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = VoyageAIRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(new VoyageAIRerankServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testFromMap_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var serviceSettings = VoyageAIRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(new VoyageAIRerankServiceSettings(TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT))));
    }

    public void testFromMap_NoModelId_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> VoyageAIRerankServiceSettings.fromMap(buildServiceSettingsMap(null, null), randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.MODEL_ID))
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new VoyageAIRerankServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT));

        assertThat(
            updatedServiceSettings,
            is(new VoyageAIRerankServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new VoyageAIRerankServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new VoyageAIRerankServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT));

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
    protected Writeable.Reader<VoyageAIRerankServiceSettings> instanceReader() {
        return VoyageAIRerankServiceSettings::new;
    }

    @Override
    protected VoyageAIRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected VoyageAIRerankServiceSettings mutateInstance(VoyageAIRerankServiceSettings instance) throws IOException {
        if (randomBoolean()) {
            var modelId = randomValueOtherThan(instance.modelId(), () -> randomAlphaOfLength(15));
            return new VoyageAIRerankServiceSettings(modelId, instance.rateLimitSettings());
        } else {
            var rateLimitSettings = randomValueOtherThan(instance.rateLimitSettings(), RateLimitSettingsTests::createRandom);
            return new VoyageAIRerankServiceSettings(instance.modelId(), rateLimitSettings);
        }
    }

    @Override
    protected VoyageAIRerankServiceSettings mutateInstanceForVersion(VoyageAIRerankServiceSettings instance, TransportVersion version) {
        return instance;
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
}
