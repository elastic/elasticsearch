/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.hamcrest.Matchers.is;

public class OpenShiftAiRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<OpenShiftAiRerankServiceSettings> {

    private static final String TEST_MODEL_ID = "model";
    private static final String TEST_URL = "http://www.abc.com";
    private static final int TEST_RATE_LIMIT = 100;
    private static final int DEFAULT_RATE_LIMIT = 3000;

    private static final String INITIAL_TEST_MODEL_ID = "initial_model";
    private static final String INITIAL_TEST_URL = "http://www.initial.com";
    private static final int INITIAL_TEST_RATE_LIMIT = 50;

    private static OpenShiftAiRerankServiceSettings createRandom() {
        var modelId = randomAlphaOfLengthOrNull(8);
        var url = randomAlphaOfLength(15);
        return new OpenShiftAiRerankServiceSettings(modelId, ServiceUtils.createUri(url), RateLimitSettingsTests.createRandom());
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var rateLimitSettings = new RateLimitSettings(TEST_RATE_LIMIT);

        assertXContentEquals(new OpenShiftAiRerankServiceSettings(TEST_MODEL_ID, TEST_URL, rateLimitSettings), Strings.format("""
            {
                "model_id":"%s",
                "url":"%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_URL, TEST_RATE_LIMIT));
    }

    public void testToXContent_WritesDefaultRateLimitAndOmitsModelIdIfNotSet() throws IOException {
        assertXContentEquals(new OpenShiftAiRerankServiceSettings(null, TEST_URL, null), Strings.format("""
            {
                "url":"%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_URL, DEFAULT_RATE_LIMIT));
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new OpenShiftAiRerankServiceSettings(
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
            is(new OpenShiftAiRerankServiceSettings(INITIAL_TEST_MODEL_ID, INITIAL_TEST_URL, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new OpenShiftAiRerankServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URL,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    private static void assertXContentEquals(OpenShiftAiRerankServiceSettings serviceSettings, String expectedString) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(expectedString));
    }

    @Override
    protected Writeable.Reader<OpenShiftAiRerankServiceSettings> instanceReader() {
        return OpenShiftAiRerankServiceSettings::new;
    }

    @Override
    protected OpenShiftAiRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenShiftAiRerankServiceSettings mutateInstance(OpenShiftAiRerankServiceSettings instance) throws IOException {
        String modelId = instance.modelId();
        URI uri = instance.uri();
        RateLimitSettings rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> ServiceUtils.createUri(randomAlphaOfLength(15)));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new OpenShiftAiRerankServiceSettings(modelId, uri, rateLimitSettings);
    }

    @Override
    protected OpenShiftAiRerankServiceSettings mutateInstanceForVersion(
        OpenShiftAiRerankServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model) {
        return new HashMap<>(OpenShiftAiChatCompletionServiceSettingsTests.getServiceSettingsMap(url, model));
    }

}
