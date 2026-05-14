/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.completion;

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

public class CohereCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<CohereCompletionServiceSettings> {

    private static final TransportVersion ML_INFERENCE_COHERE_API_VERSION = TransportVersion.fromName("ml_inference_cohere_api_version");

    private static final String TEST_URL = "https://www.test.com";
    private static final String INITIAL_TEST_URL = "https://www.initial-test.com";

    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    private static final RateLimitSettings DEFAULT_COHERE_COMPLETION_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    public static CohereCompletionServiceSettings createRandom() {
        return new CohereCompletionServiceSettings(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            RateLimitSettingsTests.createRandom(),
            randomFrom(CohereServiceSettings.CohereApiVersion.values())
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(CohereServiceSettings.CohereApiVersion.V2.toString());

        var originalServiceSettings = new CohereCompletionServiceSettings(
            INITIAL_TEST_URL,
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
            CohereServiceSettings.CohereApiVersion.V1
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new CohereCompletionServiceSettings(
                    INITIAL_TEST_URL,
                    INITIAL_TEST_MODEL_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT),
                    CohereServiceSettings.CohereApiVersion.V1
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new CohereCompletionServiceSettings(
            INITIAL_TEST_URL,
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
            CohereServiceSettings.CohereApiVersion.V1
        );
        var serviceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(serviceSettings, is(originalServiceSettings));
    }

    public void testFromMap_Persistent_EmptyMap_CreatesSettingsCorrectly() {
        var serviceSettings = CohereCompletionServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new CohereCompletionServiceSettings(
                    (String) null,
                    null,
                    DEFAULT_COHERE_COMPLETION_RATE_LIMIT_SETTINGS,
                    CohereServiceSettings.CohereApiVersion.V1
                )
            )
        );
    }

    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = CohereCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(CohereServiceSettings.CohereApiVersion.V2.toString()),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new CohereCompletionServiceSettings(
                    TEST_URL,
                    TEST_MODEL_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT),
                    CohereServiceSettings.CohereApiVersion.V2
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new CohereCompletionServiceSettings(
            TEST_URL,
            TEST_MODEL_ID,
            new RateLimitSettings(TEST_RATE_LIMIT),
            CohereServiceSettings.CohereApiVersion.V1
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
            """, TEST_URL, TEST_MODEL_ID, TEST_RATE_LIMIT, CohereServiceSettings.CohereApiVersion.V1))));
    }

    public void testToXContentFragmentOfExposedFields_DoesNotWriteApiVersion() throws IOException {
        var serviceSettings = new CohereCompletionServiceSettings(
            TEST_URL,
            TEST_MODEL_ID,
            new RateLimitSettings(TEST_RATE_LIMIT),
            CohereServiceSettings.CohereApiVersion.V1
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        serviceSettings.toXContentFragmentOfExposedFields(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
              "url": "%s",
              "model_id": "%s",
              "rate_limit": {
                "requests_per_minute": %d
              }
            }
            """, TEST_URL, TEST_MODEL_ID, TEST_RATE_LIMIT))));
    }

    private static HashMap<String, Object> buildServiceSettingsMap(@Nullable String apiVersion) {
        var result = new HashMap<String, Object>();
        result.put(ServiceFields.URL, CohereCompletionServiceSettingsTests.TEST_URL);
        result.put(ServiceFields.MODEL_ID, CohereCompletionServiceSettingsTests.TEST_MODEL_ID);
        if (apiVersion != null) {
            result.put(CohereServiceSettings.API_VERSION, apiVersion);
        }
        result.put(
            RateLimitSettings.FIELD_NAME,
            new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, CohereCompletionServiceSettingsTests.TEST_RATE_LIMIT))
        );
        return result;
    }

    @Override
    protected Writeable.Reader<CohereCompletionServiceSettings> instanceReader() {
        return CohereCompletionServiceSettings::new;
    }

    @Override
    protected CohereCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereCompletionServiceSettings mutateInstance(CohereCompletionServiceSettings instance) throws IOException {
        URI uri = instance.uri();
        var uriString = uri == null ? null : uri.toString();
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        var apiVersion = instance.apiVersion();
        switch (randomInt(3)) {
            case 0 -> uriString = randomValueOtherThan(uriString, () -> randomAlphaOfLengthOrNull(8));
            case 1 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(8));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 3 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomFrom(CohereServiceSettings.CohereApiVersion.values()));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CohereCompletionServiceSettings(uriString, modelId, rateLimitSettings, apiVersion);
    }

    @Override
    protected CohereCompletionServiceSettings mutateInstanceForVersion(CohereCompletionServiceSettings instance, TransportVersion version) {
        if (version.supports(ML_INFERENCE_COHERE_API_VERSION) == false) {
            return new CohereCompletionServiceSettings(
                instance.uri(),
                instance.modelId(),
                instance.rateLimitSettings(),
                CohereServiceSettings.CohereApiVersion.V1
            );
        }

        return instance;
    }
}
