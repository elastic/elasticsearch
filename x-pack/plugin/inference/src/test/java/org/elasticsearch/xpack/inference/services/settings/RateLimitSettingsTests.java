/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class RateLimitSettingsTests extends AbstractBWCWireSerializationTestCase<RateLimitSettings> {

    public static RateLimitSettings createRandom() {
        return new RateLimitSettings(randomLongBetween(1, 1000000));
    }

    public void testThrows_WhenGiven0() {
        expectThrows(IllegalArgumentException.class, () -> new RateLimitSettings(0));
    }

    public void testThrows_WhenGivenNegativeValue() {
        expectThrows(IllegalArgumentException.class, () -> new RateLimitSettings(-3));
    }

    public void testThrows_WhenGiven0_WithTimeUnit() {
        expectThrows(IllegalArgumentException.class, () -> new RateLimitSettings(0, TimeUnit.MILLISECONDS));
    }

    public void testThrows_WhenGivenNegativeValue_WithTimeUnit() {
        expectThrows(IllegalArgumentException.class, () -> new RateLimitSettings(-3, TimeUnit.MILLISECONDS));
    }

    public void testOf() {
        var validation = new ValidationException();
        Map<String, Object> settings = new HashMap<>(
            Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100)))
        );
        var res = RateLimitSettings.of(settings, new RateLimitSettings(1), validation, "test", ConfigurationParseContext.PERSISTENT);

        assertThat(res, is(new RateLimitSettings(100)));
        assertTrue(res.isEnabled());
        assertTrue(validation.validationErrors().isEmpty());
    }

    public void testOf_UsesDefaultValue_WhenRateLimit_IsAbsent() {
        var validation = new ValidationException();
        Map<String, Object> settings = new HashMap<>(
            Map.of("abc", new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100)))
        );
        var res = RateLimitSettings.of(settings, new RateLimitSettings(1), validation, "test", ConfigurationParseContext.PERSISTENT);

        assertThat(res, is(new RateLimitSettings(1)));
        assertTrue(res.isEnabled());
        assertTrue(validation.validationErrors().isEmpty());
    }

    public void testOf_UsesDefaultValue_WhenRequestsPerMinute_IsAbsent() {
        var validation = new ValidationException();
        Map<String, Object> settings = new HashMap<>(Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of("abc", 100))));
        var res = RateLimitSettings.of(settings, new RateLimitSettings(1), validation, "test", ConfigurationParseContext.PERSISTENT);

        assertThat(res, is(new RateLimitSettings(1)));
        assertTrue(res.isEnabled());
        assertTrue(validation.validationErrors().isEmpty());
    }

    public void testOf_ThrowsException_WithUnknownField_InRequestContext() {
        var validation = new ValidationException();
        Map<String, Object> settings = new HashMap<>(Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of("abc", 100))));

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> RateLimitSettings.of(settings, new RateLimitSettings(1), validation, "test", ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), is("Configuration contains settings [{abc=100}] unknown to the [test] service"));
    }

    public void testToXContent() throws IOException {
        var settings = new RateLimitSettings(100);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        settings.toXContent(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"rate_limit":{"requests_per_minute":100}}"""));
    }

    public void testDisableRateLimiting() {
        Map<String, Object> settings = new HashMap<>(
            Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100)))
        );
        var res = RateLimitSettings.disabledRateLimiting(settings);

        assertThat(res, is(new RateLimitSettings(1, TimeUnit.MINUTES, false)));
        assertThat(res, sameInstance(RateLimitSettings.DISABLED_INSTANCE));
        assertFalse(res.isEnabled());
        assertThat(settings, anEmptyMap());
    }

    public void testToXContent_WhenDisabled() throws IOException {
        var settings = new RateLimitSettings(1, TimeUnit.MINUTES, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        settings.toXContent(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
            }""")));
    }

    @Override
    protected Writeable.Reader<RateLimitSettings> instanceReader() {
        return RateLimitSettings::new;
    }

    @Override
    protected RateLimitSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected RateLimitSettings mutateInstance(RateLimitSettings instance) throws IOException {
        return randomValueOtherThan(instance, RateLimitSettingsTests::createRandom);
    }

    @Override
    protected RateLimitSettings mutateInstanceForVersion(RateLimitSettings instance, TransportVersion version) {
        if (version.before(TransportVersions.INFERENCE_API_DISABLE_EIS_RATE_LIMITING)) {
            return new RateLimitSettings(instance.requestsPerTimeUnit(), instance.timeUnit(), true);
        } else {
            return instance;
        }
    }
}
