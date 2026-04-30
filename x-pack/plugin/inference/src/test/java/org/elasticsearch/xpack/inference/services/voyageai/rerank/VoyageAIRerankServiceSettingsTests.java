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
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettingsTests.DEFAULT_RATE_LIMIT;
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettingsTests.INITIAL_TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettingsTests.INITIAL_TEST_RATE_LIMIT;
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettingsTests.TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAICommonServiceSettingsTests.TEST_RATE_LIMIT;
import static org.hamcrest.Matchers.is;

public class VoyageAIRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<VoyageAIRerankServiceSettings> {

    public static VoyageAIRerankServiceSettings createRandom() {
        return new VoyageAIRerankServiceSettings(VoyageAICommonServiceSettingsTests.createRandom());
    }

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = VoyageAIRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(new VoyageAIRerankServiceSettings(new VoyageAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))))
        );
    }

    public void testFromMap_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var serviceSettings = VoyageAIRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new VoyageAIRerankServiceSettings(
                    new VoyageAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT))
                )
            )
        );
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
            new VoyageAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT))
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT));

        assertThat(
            updatedServiceSettings,
            is(
                new VoyageAIRerankServiceSettings(
                    new VoyageAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new VoyageAIRerankServiceSettings(
            new VoyageAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT))
        );

        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new VoyageAIRerankServiceSettings(
            new VoyageAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))
        );

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
        var commonSettings = randomValueOtherThan(instance.getCommonSettings(), VoyageAICommonServiceSettingsTests::createRandom);
        return new VoyageAIRerankServiceSettings(commonSettings);
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
