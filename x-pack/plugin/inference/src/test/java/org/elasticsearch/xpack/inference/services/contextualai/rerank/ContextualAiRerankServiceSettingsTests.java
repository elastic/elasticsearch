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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ContextualAiRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<ContextualAiRerankServiceSettings> {

    private static final String TEST_URL = "http://www.abc.com";
    private static final String INITIAL_TEST_URL = "http://initial.contextual.local";
    private static final String TEST_MODEL_ID = "some_model";
    private static final String INITIAL_TEST_MODEL_ID = "initial_model";
    private static final String DEFAULT_RERANK_URL = "https://api.contextual.ai/v1/rerank";
    private static final String TEST_INVALID_URL = "^^^";
    private static final int TEST_RATE_LIMIT = 2;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;
    private static final int DEFAULT_RATE_LIMIT = 1000;

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = createRerankServiceSettings(
            createUri(INITIAL_TEST_URL),
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            createServiceSettingsMap(TEST_MODEL_ID, TEST_URL, TEST_RATE_LIMIT)
        );

        assertThat(
            updatedServiceSettings,
            is(createRerankServiceSettings(createUri(INITIAL_TEST_URL), INITIAL_TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = createRerankServiceSettings(
            createUri(INITIAL_TEST_URL),
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testFromMap_Request_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var serviceSettings = ContextualAiRerankServiceSettings.fromMap(
            createServiceSettingsMap(TEST_MODEL_ID, TEST_URL, null),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(createRerankServiceSettings(createUri(TEST_URL), TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_Request_AllFields_CreatesSettingsCorrectly() {
        var settingsMap = createServiceSettingsMap(TEST_MODEL_ID, TEST_URL, TEST_RATE_LIMIT);

        var serviceSettings = ContextualAiRerankServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(createRerankServiceSettings(createUri(TEST_URL), TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = ContextualAiRerankServiceSettings.fromMap(
            createServiceSettingsMap(TEST_MODEL_ID, TEST_URL, TEST_RATE_LIMIT),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(createRerankServiceSettings(createUri(TEST_URL), TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_Persistent_NoModelId_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> ContextualAiRerankServiceSettings.fromMap(
                createServiceSettingsMap(null, TEST_URL, TEST_RATE_LIMIT),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [model_id];")
        );
    }

    public void testFromMap_Persistent_NoUrl_CreatesSettingsCorrectly() {
        var serviceSettings = ContextualAiRerankServiceSettings.fromMap(
            createServiceSettingsMap(TEST_MODEL_ID, null, TEST_RATE_LIMIT),
            ConfigurationParseContext.PERSISTENT
        );
        assertThat(
            serviceSettings,
            is(createRerankServiceSettings(createUri(DEFAULT_RERANK_URL), TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_Persistent_InvalidUrl_ThrowsException() {
        assertFromMapPersistentInvalidUrl(createServiceSettingsMap(TEST_MODEL_ID, TEST_INVALID_URL, TEST_RATE_LIMIT), Strings.format("""
            Validation Failed: 1: [service_settings] Invalid url [%s] received for field [url]. \
            Error: unable to parse url [%s]. Reason: Illegal character in path;""", TEST_INVALID_URL, TEST_INVALID_URL));
    }

    public void testFromMap_Persistent_EmptyUrl_ThrowsException() {
        assertFromMapPersistentInvalidUrl(
            createServiceSettingsMap(TEST_MODEL_ID, "", TEST_RATE_LIMIT),
            "Validation Failed: 1: [service_settings] Invalid value empty string. [url] must be a non-empty string;"
        );
    }

    private static void assertFromMapPersistentInvalidUrl(Map<String, Object> serviceSettingsMap, String expectedErrorMessage) {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> ContextualAiRerankServiceSettings.fromMap(new HashMap<>(serviceSettingsMap), ConfigurationParseContext.PERSISTENT)
        );

        assertThat(thrownException.getMessage(), containsString(expectedErrorMessage));
    }

    public void testFromMap_Persistent_NoRateLimit_CreatesSettingsCorrectly() {
        var serviceSettings = ContextualAiRerankServiceSettings.fromMap(
            createServiceSettingsMap(TEST_MODEL_ID, TEST_URL, null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(createRerankServiceSettings(createUri(TEST_URL), TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var settings = createRerankServiceSettings(createUri(TEST_URL), TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
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

    public void testToXContent_DefaultUrl_DefaultRateLimit() throws IOException {
        var settings = createRerankServiceSettings(createUri(DEFAULT_RERANK_URL), TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "url": "%s",
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, DEFAULT_RERANK_URL, TEST_MODEL_ID, DEFAULT_RATE_LIMIT))));
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
        var uri = instance.uri();
        var rateLimitSettings = instance.rateLimitSettings();

        switch (randomInt(2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> createUri(randomAlphaOfLength(15)));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return createRerankServiceSettings(uri, modelId, rateLimitSettings);
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
        return createRerankServiceSettings(createUri(randomAlphaOfLength(15)), modelId, RateLimitSettingsTests.createRandom());
    }

    private static ContextualAiRerankServiceSettings createRerankServiceSettings(
        URI uri,
        String modelId,
        RateLimitSettings rateLimitSettings
    ) {
        return new ContextualAiRerankServiceSettings(new ContextualAiServiceSettings.CommonSettings(uri, modelId, rateLimitSettings));
    }

    /**
     * Builds a service settings map. When {@code rateLimit} is null, no {@code rate_limit} object is added.
     */
    public static HashMap<String, Object> createServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable Integer rateLimit
    ) {
        var result = new HashMap<String, Object>();
        if (modelId != null) {
            result.put(ServiceFields.MODEL_ID, modelId);
        }
        if (url != null) {
            result.put(ServiceFields.URL, url);
        }
        if (rateLimit != null) {
            result.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return result;
    }
}
