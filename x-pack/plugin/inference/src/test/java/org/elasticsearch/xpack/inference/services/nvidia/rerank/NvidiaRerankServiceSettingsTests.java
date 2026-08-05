/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.rerank;

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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.is;

public class NvidiaRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<NvidiaRerankServiceSettings> {

    private static final URI TEST_URI = URI.create("https://www.test.com");
    private static final URI INITIAL_TEST_URI = URI.create("https://www.initial.com");
    private static final URI DEFAULT_URI = URI.create("https://ai.api.nvidia.com/v1/retrieval/nvidia/reranking");

    private static final String TEST_MODEL_ID = "test-model";
    private static final String INITIAL_TEST_MODEL_ID = "initial-model";

    private static final int TEST_RATE_LIMIT = 500;
    private static final int INITIAL_TEST_RATE_LIMIT = 100;
    private static final int DEFAULT_RATE_LIMIT = 3000;

    private static final String INVALID_TEST_URL = "^^^";

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = NvidiaRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(new NvidiaRerankServiceSettings(TEST_MODEL_ID, TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testFromMap_OnlyMandatoryFields_UsesDefaultValues_Success() {
        var serviceSettings = NvidiaRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(new NvidiaRerankServiceSettings(TEST_MODEL_ID, DEFAULT_URI, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_NoModelId_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaRerankServiceSettings.fromMap(
                buildServiceSettingsMap(null, TEST_URI.toString(), TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.MODEL_ID))
        );
    }

    public void testFromMap_InvalidUrl_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaRerankServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, INVALID_TEST_URL, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(thrownException.validationErrors().getFirst(), is(Strings.format("""
            [service_settings] Invalid url [%s] received for field [%s]. \
            Error: unable to parse url [%s]. Reason: Illegal character in path""", INVALID_TEST_URL, ServiceFields.URL, INVALID_TEST_URL)));
    }

    public void testFromMap_EmptyUrl_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaRerankServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, "", TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value empty string. [%s] must be a non-empty string", ServiceFields.URL))
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), TEST_RATE_LIMIT);
        var originalServiceSettings = new NvidiaRerankServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(new NvidiaRerankServiceSettings(INITIAL_TEST_MODEL_ID, INITIAL_TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new NvidiaRerankServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new NvidiaRerankServiceSettings(TEST_MODEL_ID, TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "url": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_URI.toString(), TEST_RATE_LIMIT));

        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<NvidiaRerankServiceSettings> instanceReader() {
        return NvidiaRerankServiceSettings::new;
    }

    @Override
    protected NvidiaRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected NvidiaRerankServiceSettings mutateInstance(NvidiaRerankServiceSettings instance) throws IOException {
        String modelId = instance.modelId();
        URI uri = instance.uri();
        RateLimitSettings rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> createUri(randomAlphaOfLength(15)));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new NvidiaRerankServiceSettings(modelId, uri, rateLimitSettings);
    }

    @Override
    protected NvidiaRerankServiceSettings mutateInstanceForVersion(NvidiaRerankServiceSettings instance, TransportVersion version) {
        return instance;
    }

    private static NvidiaRerankServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);
        var url = randomAlphaOfLengthOrNull(15);
        return new NvidiaRerankServiceSettings(modelId, createOptionalUri(url), RateLimitSettingsTests.createRandom());
    }

    /**
     * Helper method to build a service settings map.
     * @param modelId the model ID
     * @param url the service URL
     * @param rateLimit the rate limit value (requests per minute)
     * @return a map representing the service settings
     */
    public static Map<String, Object> buildServiceSettingsMap(@Nullable String modelId, @Nullable String url, @Nullable Integer rateLimit) {
        var map = new HashMap<String, Object>();
        if (modelId != null) {
            map.put(ServiceFields.MODEL_ID, modelId);
        }
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }

}
