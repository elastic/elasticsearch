/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CohereRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<CohereRerankServiceSettings> {

    private static final TransportVersion ML_INFERENCE_COHERE_API_VERSION = TransportVersion.fromName("ml_inference_cohere_api_version");

    private static final String TEST_URL = "https://www.test.com";
    private static final String INITIAL_TEST_URL = "https://www.initial-test.com";

    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    private static final RateLimitSettings DEFAULT_COHERE_RERANK_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    public static CohereRerankServiceSettings createRandom() {
        return new CohereRerankServiceSettings(
            randomAlphaOfLengthOrNull(10),
            randomAlphaOfLengthOrNull(10),
            randomFrom(RateLimitSettingsTests.createRandom(), null),
            randomFrom(CohereServiceSettings.CohereApiVersion.values())
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new CohereRerankServiceSettings(
            INITIAL_TEST_URL,
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
            CohereServiceSettings.CohereApiVersion.V1
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            buildServiceSettingsMap(CohereServiceSettings.CohereApiVersion.V2.toString())
        );

        assertThat(
            updatedServiceSettings,
            is(
                new CohereRerankServiceSettings(
                    INITIAL_TEST_URL,
                    INITIAL_TEST_MODEL_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT),
                    CohereServiceSettings.CohereApiVersion.V1
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new CohereRerankServiceSettings(
            INITIAL_TEST_URL,
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
            CohereServiceSettings.CohereApiVersion.V1
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testFromMap_Persistent_EmptyMap_CreatesSettingsCorrectly() {
        var serviceSettings = CohereRerankServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new CohereRerankServiceSettings(
                    (String) null,
                    null,
                    DEFAULT_COHERE_RERANK_RATE_LIMIT_SETTINGS,
                    CohereServiceSettings.CohereApiVersion.V1
                )
            )
        );
    }

    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = CohereRerankServiceSettings.fromMap(
            buildServiceSettingsMap(CohereServiceSettings.CohereApiVersion.V2.toString()),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new CohereRerankServiceSettings(
                    TEST_URL,
                    TEST_MODEL_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT),
                    CohereServiceSettings.CohereApiVersion.V2
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new CohereRerankServiceSettings(
            TEST_URL,
            TEST_MODEL_ID,
            new RateLimitSettings(TEST_RATE_LIMIT),
            CohereServiceSettings.CohereApiVersion.V2
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
              "url": "%s",
              "model_id": "%s",
              "rate_limit": {
                "requests_per_minute": %d
              },
              "api_version": "%s"
            }
            """, TEST_URL, TEST_MODEL_ID, TEST_RATE_LIMIT, CohereServiceSettings.CohereApiVersion.V2))));
    }

    private static HashMap<String, Object> buildServiceSettingsMap(@Nullable String apiVersion) {
        var result = new HashMap<String, Object>();
        result.put(ServiceFields.URL, CohereRerankServiceSettingsTests.TEST_URL);
        result.put(ServiceFields.MODEL_ID, CohereRerankServiceSettingsTests.TEST_MODEL_ID);
        if (apiVersion != null) {
            result.put(CohereServiceSettings.API_VERSION, apiVersion);
        }
        result.put(
            RateLimitSettings.FIELD_NAME,
            new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, CohereRerankServiceSettingsTests.TEST_RATE_LIMIT))
        );
        return result;
    }

    @Override
    protected Writeable.Reader<CohereRerankServiceSettings> instanceReader() {
        return CohereRerankServiceSettings::new;
    }

    @Override
    protected CohereRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereRerankServiceSettings mutateInstance(CohereRerankServiceSettings instance) throws IOException {
        URI uri = instance.uri();
        var uriString = uri == null ? null : uri.toString();
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        var apiVersion = instance.apiVersion();
        switch (randomInt(3)) {
            case 0 -> uriString = randomValueOtherThan(uriString, () -> randomAlphaOfLengthOrNull(8));
            case 1 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(10));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 3 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomFrom(CohereServiceSettings.CohereApiVersion.values()));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CohereRerankServiceSettings(uriString, modelId, rateLimitSettings, apiVersion);
    }

    @Override
    protected CohereRerankServiceSettings mutateInstanceForVersion(CohereRerankServiceSettings instance, TransportVersion version) {
        if (version.supports(ML_INFERENCE_COHERE_API_VERSION) == false) {
            return new CohereRerankServiceSettings(
                instance.uri(),
                instance.modelId(),
                instance.rateLimitSettings(),
                CohereServiceSettings.CohereApiVersion.V1
            );
        }
        return instance;
    }
}
