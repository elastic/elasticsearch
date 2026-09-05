/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.common.Strings;
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

/**
 * Base test case for {@link TencentCloudCommonServiceSettings} subclasses. Holds the assertions for the fields common to every
 * TencentCloud task (model identity, region, and rate limiting) so they are exercised once for each task type instead of being
 * duplicated in every concrete settings test. Task-specific tests live in the concrete subclasses.
 */
public abstract class AbstractTencentCloudServiceSettingsTests<T extends TencentCloudCommonServiceSettings> extends
    AbstractBWCWireSerializationTestCase<T> {

    protected static final String TEST_MODEL_ID = "bge-m3";
    protected static final String INITIAL_TEST_MODEL_ID = "bge-large-zh-v1.5";
    protected static final String TEST_REGION = "gz";
    protected static final int TEST_RATE_LIMIT = 100;
    protected static final int INITIAL_TEST_RATE_LIMIT = 30;

    /**
     * Creates a settings instance with the given common fields and defaults for any task-specific fields.
     */
    protected abstract TencentCloudCommonServiceSettings createInstance(String modelId, String region, RateLimitSettings rateLimitSettings);

    public void testConstructor_NullRegion_UsesDefault() {
        var settings = createInstance(TEST_MODEL_ID, null, null);
        assertThat(settings.region(), is(TencentCloudUtils.DEFAULT_REGION));
    }

    public void testConstructor_ExplicitRegion() {
        var settings = createInstance(TEST_MODEL_ID, TEST_REGION, null);
        assertThat(settings.region(), is(TEST_REGION));
    }

    public void testUpdateServiceSettings_OnlyRateLimitIsMutable() {
        var originalSettings = createInstance(INITIAL_TEST_MODEL_ID, TEST_REGION, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));
        var updatedSettings = originalSettings.updateServiceSettings(
            new HashMap<>(
                Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)))
            )
        );

        // model id and region are immutable
        assertThat(updatedSettings.modelId(), is(INITIAL_TEST_MODEL_ID));
        assertThat(updatedSettings.region(), is(TEST_REGION));
        assertThat(updatedSettings.rateLimitSettings(), is(new RateLimitSettings(TEST_RATE_LIMIT)));
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalSettings = createInstance(INITIAL_TEST_MODEL_ID, TEST_REGION, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));
        var updatedSettings = originalSettings.updateServiceSettings(new HashMap<>());
        assertThat(updatedSettings, is(originalSettings));
    }

    public void testXContent_WithRegion() throws IOException {
        var settings = createInstance(TEST_MODEL_ID, TEST_REGION, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","region":"%s","rate_limit":{"requests_per_minute":%d}}""", TEST_MODEL_ID, TEST_REGION, TEST_RATE_LIMIT)));
    }

    public void testXContent_DefaultRegion() throws IOException {
        var settings = createInstance(TEST_MODEL_ID, null, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","region":"bj","rate_limit":{"requests_per_minute":%d}}""", TEST_MODEL_ID, TEST_RATE_LIMIT)));
    }

    @Override
    protected abstract Writeable.Reader<T> instanceReader();

    @Override
    protected abstract T createTestInstance();

    @Override
    protected abstract T mutateInstance(T instance) throws IOException;

    @Override
    protected abstract T mutateInstanceForVersion(T instance, org.elasticsearch.TransportVersion version);
}
