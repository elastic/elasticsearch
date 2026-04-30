/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class OpenShiftAiChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    OpenShiftAiChatCompletionServiceSettings> {

    private static final String TEST_MODEL_ID = "some_model";
    private static final String INITIAL_TEST_MODEL_ID = "initial_model";

    private static final String TEST_URL = "http://www.abc.com";
    private static final String INITIAL_TEST_URL = "http://www.initial.com";
    private static final String INVALID_TEST_URL = "^^^";

    private static final int TEST_RATE_LIMIT = 2;
    private static final int INITIAL_TEST_RATE_LIMIT = 50;
    private static final int DEFAULT_RATE_LIMIT = 3000;

    public void testFromMap_AllFields_CreatesInstanceSuccessfully() {
        var serviceSettings = OpenShiftAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    TEST_MODEL_ID,
                    ServiceFields.URL,
                    TEST_URL,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
                )
            ),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(new OpenShiftAiChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URL, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_OnlyMandatoryFields_CreatesInstanceSuccessfully() {
        var serviceSettings = OpenShiftAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.URL, TEST_URL)),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(new OpenShiftAiChatCompletionServiceSettings(null, TEST_URL, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingUrl_ThrowsException() {
        testFromMap_InvalidUrl(
            Map.of(
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            ),
            "Validation Failed: 1: [service_settings] does not contain the required setting [url];"
        );
    }

    public void testFromMap_InvalidUrl_ThrowsException() {
        testFromMap_InvalidUrl(
            Map.of(
                ServiceFields.URL,
                INVALID_TEST_URL,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            ),
            """
                Validation Failed: 1: [service_settings] Invalid url [^^^] received for field [url]. \
                Error: unable to parse url [^^^]. Reason: Illegal character in path;"""
        );
    }

    public void testFromMap_EmptyUrl_ThrowsException() {
        testFromMap_InvalidUrl(
            Map.of(
                ServiceFields.URL,
                "",
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            ),
            "Validation Failed: 1: [service_settings] Invalid value empty string. [url] must be a non-empty string;"
        );
    }

    private static void testFromMap_InvalidUrl(Map<String, Object> serviceSettingsMap, String expectedErrorMessage) {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenShiftAiChatCompletionServiceSettings.fromMap(
                new HashMap<>(serviceSettingsMap),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.getMessage(), containsString(expectedErrorMessage));
    }

    public void testFromMap_MissingRateLimit_Success() {
        var serviceSettings = OpenShiftAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID, ServiceFields.URL, TEST_URL)),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(new OpenShiftAiChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URL, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new OpenShiftAiChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URL, new RateLimitSettings(TEST_RATE_LIMIT));

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
            """, TEST_MODEL_ID, TEST_URL, TEST_RATE_LIMIT));

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_DoesNotWriteOptionalValues_DefaultRateLimit() throws IOException {
        var serviceSettings = new OpenShiftAiChatCompletionServiceSettings(null, TEST_URL, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "url": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_URL, DEFAULT_RATE_LIMIT));
        assertThat(xContentResult, is(expected));
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new OpenShiftAiChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URL,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    TEST_MODEL_ID,
                    ServiceFields.URL,
                    TEST_URL,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
                )
            )
        );

        assertThat(
            updatedServiceSettings,
            is(
                new OpenShiftAiChatCompletionServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_URL,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new OpenShiftAiChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URL,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    @Override
    protected Writeable.Reader<OpenShiftAiChatCompletionServiceSettings> instanceReader() {
        return OpenShiftAiChatCompletionServiceSettings::new;
    }

    @Override
    protected OpenShiftAiChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenShiftAiChatCompletionServiceSettings mutateInstance(OpenShiftAiChatCompletionServiceSettings instance)
        throws IOException {
        String modelId = instance.modelId();
        URI uri = instance.uri();
        RateLimitSettings rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> ServiceUtils.createUri(randomAlphaOfLength(15)));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new OpenShiftAiChatCompletionServiceSettings(modelId, uri, rateLimitSettings);
    }

    @Override
    protected OpenShiftAiChatCompletionServiceSettings mutateInstanceForVersion(
        OpenShiftAiChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static OpenShiftAiChatCompletionServiceSettings createRandom() {
        var modelId = randomAlphaOfLengthOrNull(8);
        var url = randomAlphaOfLength(15);
        return new OpenShiftAiChatCompletionServiceSettings(modelId, ServiceUtils.createUri(url), RateLimitSettingsTests.createRandom());
    }

    public static Map<String, Object> getServiceSettingsMap(String model, String url) {
        var map = new HashMap<String, Object>();

        map.put(ServiceFields.MODEL_ID, model);
        map.put(ServiceFields.URL, url);

        return map;
    }

}
