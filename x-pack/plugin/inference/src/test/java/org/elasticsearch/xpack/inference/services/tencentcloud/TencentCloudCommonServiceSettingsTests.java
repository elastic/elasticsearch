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
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class TencentCloudCommonServiceSettingsTests extends AbstractBWCWireSerializationTestCase<TencentCloudCommonServiceSettings> {

    private static final String TEST_MODEL_ID = "bge-m3";
    private static final String INITIAL_TEST_MODEL_ID = "bge-large-zh-v1.5";
    private static final String TEST_REGION = "gz";
    private static final int TEST_RATE_LIMIT = 100;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public static TencentCloudCommonServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(10);
        var region = randomBoolean() ? randomAlphaOfLength(5) : TencentCloudUtils.DEFAULT_REGION;
        var rateLimitSettings = randomBoolean() ? new RateLimitSettings(randomIntBetween(1, 1000)) : null;
        return new TencentCloudCommonServiceSettings(modelId, region, rateLimitSettings);
    }

    public void testConstructor_NullRegion_UsesDefault() {
        var settings = new TencentCloudCommonServiceSettings(TEST_MODEL_ID, null, null);
        assertThat(settings.region(), is(TencentCloudUtils.DEFAULT_REGION));
    }

    public void testConstructor_ExplicitRegion() {
        var settings = new TencentCloudCommonServiceSettings(TEST_MODEL_ID, TEST_REGION, null);
        assertThat(settings.region(), is(TEST_REGION));
    }

    public void testUpdateServiceSettings_OnlyRateLimitIsMutable() {
        var originalSettings = new TencentCloudCommonServiceSettings(
            INITIAL_TEST_MODEL_ID,
            TEST_REGION,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedSettings = originalSettings.updateCommonServiceSettings(
            new HashMap<>(
                Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)))
            ),
            new ValidationException()
        );

        // model id and region are immutable
        assertThat(updatedSettings.modelId(), is(INITIAL_TEST_MODEL_ID));
        assertThat(updatedSettings.region(), is(TEST_REGION));
        assertThat(updatedSettings.rateLimitSettings(), is(new RateLimitSettings(TEST_RATE_LIMIT)));
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalSettings = new TencentCloudCommonServiceSettings(
            INITIAL_TEST_MODEL_ID,
            TEST_REGION,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedSettings = originalSettings.updateCommonServiceSettings(new HashMap<>(), new ValidationException());
        assertThat(updatedSettings, is(originalSettings));
    }

    public void testXContent_WithRegion() throws IOException {
        var settings = new TencentCloudCommonServiceSettings(TEST_MODEL_ID, TEST_REGION, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","region":"%s","rate_limit":{"requests_per_minute":%d}}""", TEST_MODEL_ID, TEST_REGION, TEST_RATE_LIMIT)));
    }

    public void testXContent_DefaultRegion() throws IOException {
        var settings = new TencentCloudCommonServiceSettings(TEST_MODEL_ID, null, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","region":"bj","rate_limit":{"requests_per_minute":%d}}""", TEST_MODEL_ID, TEST_RATE_LIMIT)));
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
        var region = instance.region();
        var rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> region = randomValueOtherThan(region, () -> randomAlphaOfLength(5));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, () -> new RateLimitSettings(randomIntBetween(1, 1000)));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new TencentCloudCommonServiceSettings(modelId, region, rateLimitSettings);
    }

    @Override
    protected TencentCloudCommonServiceSettings mutateInstanceForVersion(TencentCloudCommonServiceSettings instance, TransportVersion v) {
        return instance;
    }
}
