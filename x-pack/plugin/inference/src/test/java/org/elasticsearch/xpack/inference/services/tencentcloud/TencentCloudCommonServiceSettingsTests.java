/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class TencentCloudCommonServiceSettingsTests extends AbstractBWCWireSerializationTestCase<TencentCloudCommonServiceSettings> {

    private static final String TEST_MODEL_ID = "bge-m3";
    private static final String INITIAL_TEST_MODEL_ID = "bge-large-zh-v1.5";
    private static final String TEST_URL = "http://custom.example.com/v1/embeddings";
    private static final int TEST_RATE_LIMIT = 100;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public static TencentCloudCommonServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(10);
        URI uri = randomBoolean() ? URI.create("http://" + randomAlphaOfLength(8) + "/v1/embeddings") : null;
        var rateLimitSettings = randomBoolean() ? new RateLimitSettings(randomIntBetween(1, 1000)) : null;
        return new TencentCloudCommonServiceSettings(modelId, uri, rateLimitSettings);
    }

    public void testFromMap_MinimalConfig_UsesDefaults() {
        var validationException = new ValidationException();
        var settings = TencentCloudCommonServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID)),
            ConfigurationParseContext.PERSISTENT,
            validationException
        );

        assertThat(validationException.validationErrors().size(), is(0));
        assertThat(settings.modelId(), is(TEST_MODEL_ID));
        assertNull(settings.uri());
        assertThat(settings.rateLimitSettings(), is(TencentCloudCommonServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS));
    }

    public void testFromMap_WithUrlAndRateLimit_Success() {
        var validationException = new ValidationException();
        var settings = TencentCloudCommonServiceSettings.fromMap(
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
            ConfigurationParseContext.PERSISTENT,
            validationException
        );

        assertThat(settings.modelId(), is(TEST_MODEL_ID));
        assertThat(settings.uri(), is(URI.create(TEST_URL)));
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(TEST_RATE_LIMIT)));
    }

    public void testFromMap_MissingModelId_AddsValidationError() {
        var validationException = new ValidationException();
        TencentCloudCommonServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.PERSISTENT, validationException);
        assertThat(validationException.validationErrors().size(), is(1));
    }

    public void testUpdateServiceSettings_OnlyRateLimitIsMutable() {
        var originalSettings = new TencentCloudCommonServiceSettings(
            INITIAL_TEST_MODEL_ID,
            URI.create(TEST_URL),
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedSettings = originalSettings.updateCommonServiceSettings(
            new HashMap<>(
                Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)))
            ),
            new ValidationException()
        );

        // model id and uri are immutable
        assertThat(updatedSettings.modelId(), is(INITIAL_TEST_MODEL_ID));
        assertThat(updatedSettings.uri(), is(URI.create(TEST_URL)));
        assertThat(updatedSettings.rateLimitSettings(), is(new RateLimitSettings(TEST_RATE_LIMIT)));
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalSettings = new TencentCloudCommonServiceSettings(
            INITIAL_TEST_MODEL_ID,
            null,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedSettings = originalSettings.updateCommonServiceSettings(new HashMap<>(), new ValidationException());
        assertThat(updatedSettings, is(originalSettings));
    }

    public void testXContent_WithUrl() throws IOException {
        var settings = new TencentCloudCommonServiceSettings(TEST_MODEL_ID, URI.create(TEST_URL), new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","url":"%s","rate_limit":{"requests_per_minute":%d}}""", TEST_MODEL_ID, TEST_URL, TEST_RATE_LIMIT)));
    }

    public void testXContent_WithoutUrl() throws IOException {
        var settings = new TencentCloudCommonServiceSettings(TEST_MODEL_ID, null, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","rate_limit":{"requests_per_minute":%d}}""", TEST_MODEL_ID, TEST_RATE_LIMIT)));
    }

    @Override
    protected Writeable.Reader<TencentCloudCommonServiceSettings> instanceReader() {
        return TencentCloudCommonServiceSettings::new;
    }

    @Override
    protected TencentCloudCommonServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected TencentCloudCommonServiceSettings mutateInstance(TencentCloudCommonServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var uri = instance.uri();
        var rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = uri == null ? URI.create("http://" + randomAlphaOfLength(6) + "/v1") : null;
            case 2 -> rateLimitSettings = randomValueOtherThan(
                rateLimitSettings,
                () -> new RateLimitSettings(randomIntBetween(1, 1000))
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new TencentCloudCommonServiceSettings(modelId, uri, rateLimitSettings);
    }

    @Override
    protected TencentCloudCommonServiceSettings mutateInstanceForVersion(TencentCloudCommonServiceSettings instance, TransportVersion v) {
        return instance;
    }
}
