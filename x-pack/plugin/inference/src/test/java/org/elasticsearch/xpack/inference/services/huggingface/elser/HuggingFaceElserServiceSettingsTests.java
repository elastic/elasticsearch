/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class HuggingFaceElserServiceSettingsTests extends AbstractWireSerializingTestCase<HuggingFaceElserServiceSettings> {

    public static HuggingFaceElserServiceSettings createRandom() {
        return new HuggingFaceElserServiceSettings(randomAlphaOfLength(15));
    }

    private static final URI TEST_URI = ServiceUtils.createUri("https://test-uri.com");
    private static final URI INITIAL_TEST_URI = ServiceUtils.createUri("https://initial-test-uri.com");
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                ServiceFields.URL,
                TEST_URI.toString(),
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );

        var serviceSettings = createInitialServiceSettings().updateServiceSettings(settingsMap, TaskType.SPARSE_EMBEDDING);

        assertThat(serviceSettings, is(new HuggingFaceElserServiceSettings(TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = createInitialServiceSettings().updateServiceSettings(new HashMap<>(), TaskType.SPARSE_EMBEDDING);

        assertThat(serviceSettings, is(createInitialServiceSettings()));
    }

    private static HuggingFaceElserServiceSettings createInitialServiceSettings() {
        return new HuggingFaceElserServiceSettings(INITIAL_TEST_URI, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));
    }

    public void testFromMap() {
        var serviceSettings = HuggingFaceElserServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.URL, TEST_URI.toString())),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(new HuggingFaceElserServiceSettings(TEST_URI.toString()), is(serviceSettings));
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        String emptyUrl = "";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceElserServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, emptyUrl)),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    ServiceFields.URL
                )
            )
        );
    }

    public void testFromMap_MissingUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceElserServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.PERSISTENT)
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("Validation Failed: 1: [service_settings] does not contain the required setting [%s];", ServiceFields.URL)
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var invalidUrl = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceElserServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, invalidUrl)),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s]",
                    invalidUrl,
                    ServiceFields.URL
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new HuggingFaceElserServiceSettings(TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = org.elasticsearch.common.Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"url":"%s","max_input_tokens":512,"rate_limit":{"requests_per_minute":%d}}""", TEST_URI, TEST_RATE_LIMIT)));
    }

    @Override
    protected Writeable.Reader<HuggingFaceElserServiceSettings> instanceReader() {
        return HuggingFaceElserServiceSettings::new;
    }

    @Override
    protected HuggingFaceElserServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected HuggingFaceElserServiceSettings mutateInstance(HuggingFaceElserServiceSettings instance) throws IOException {
        if (randomBoolean()) {
            var uri = randomValueOtherThan(instance.uri(), () -> createUri(randomAlphaOfLength(15)));
            return new HuggingFaceElserServiceSettings(uri, instance.rateLimitSettings());
        } else {
            var rateLimitSettings = randomValueOtherThan(instance.rateLimitSettings(), RateLimitSettingsTests::createRandom);
            return new HuggingFaceElserServiceSettings(instance.uri(), rateLimitSettings);
        }
    }
}
