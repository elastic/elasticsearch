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

/**
 * {@code fromMap} tests that use {@link ConfigurationParseContext#PERSISTENT} are named {@code testFromMap_Persistent_*}
 * (Azure AI Studio service-settings tests use {@code testFromMap_Request_*} where parsing runs in request context).
 */
public class CohereRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<CohereRerankServiceSettings> {

    private static final TransportVersion ML_INFERENCE_COHERE_API_VERSION = TransportVersion.fromName("ml_inference_cohere_api_version");

    private static final String TEST_URL = "https://www.test.com";
    private static final String INITIAL_TEST_URL = "https://www.initial-test.com";

    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    private static final CohereServiceSettings.CohereApiVersion TEST_COHERE_API_VERSION = CohereServiceSettings.CohereApiVersion.V2;
    private static final CohereServiceSettings.CohereApiVersion INITIAL_TEST_COHERE_API_VERSION = CohereServiceSettings.CohereApiVersion.V1;
    // Mirrors CohereServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS (used by CohereRerankServiceSettings#fromMap).
    private static final RateLimitSettings DEFAULT_COHERE_RERANK_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    public static CohereRerankServiceSettings createRandom() {
        return createRandom(randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() }));
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(TEST_URL, TEST_MODEL_ID, TEST_COHERE_API_VERSION.toString(), TEST_RATE_LIMIT);

        var originalServiceSettings = new CohereRerankServiceSettings(
            INITIAL_TEST_URL,
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
            INITIAL_TEST_COHERE_API_VERSION
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new CohereRerankServiceSettings(
                    INITIAL_TEST_URL,
                    INITIAL_TEST_MODEL_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT),
                    INITIAL_TEST_COHERE_API_VERSION
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new CohereRerankServiceSettings(
            INITIAL_TEST_URL,
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
            INITIAL_TEST_COHERE_API_VERSION
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
                    (String) null,
                    DEFAULT_COHERE_RERANK_RATE_LIMIT_SETTINGS,
                    INITIAL_TEST_COHERE_API_VERSION
                )
            )
        );
    }

    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = CohereRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_URL, TEST_MODEL_ID, TEST_COHERE_API_VERSION.toString(), TEST_RATE_LIMIT),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new CohereRerankServiceSettings(TEST_URL, TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT), TEST_COHERE_API_VERSION))
        );
    }

    public static CohereRerankServiceSettings createRandom(@Nullable RateLimitSettings rateLimitSettings) {
        return new CohereRerankServiceSettings(
            randomFrom(new String[] { null, Strings.format("http://%s.com", randomAlphaOfLength(8)) }),
            randomAlphaOfLengthOrNull(10),
            rateLimitSettings,
            CohereServiceSettings.CohereApiVersion.V2
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new CohereRerankServiceSettings(
            TEST_URL,
            TEST_MODEL_ID,
            new RateLimitSettings(TEST_RATE_LIMIT),
            TEST_COHERE_API_VERSION
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
            """, TEST_URL, TEST_MODEL_ID, TEST_RATE_LIMIT, TEST_COHERE_API_VERSION))));
    }

    private static HashMap<String, Object> buildServiceSettingsMap(
        @Nullable String url,
        @Nullable String modelId,
        @Nullable String apiVersion,
        @Nullable Integer rateLimit
    ) {
        var result = new HashMap<String, Object>();
        if (url != null) {
            result.put(ServiceFields.URL, url);
        }
        if (modelId != null) {
            result.put(ServiceFields.MODEL_ID, modelId);
        }
        if (apiVersion != null) {
            result.put(CohereServiceSettings.API_VERSION, apiVersion);
        }
        if (rateLimit != null) {
            result.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
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
