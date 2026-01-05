/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class RateLimitSettingsTests extends AbstractBWCWireSerializationTestCase<RateLimitSettings> {

    private static final TransportVersion INFERENCE_API_DISABLE_EIS_RATE_LIMITING = TransportVersion.fromName(
        "inference_api_disable_eis_rate_limiting"
    );

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

    public void testRejectRateLimitFieldForRequestContext_DoesNotAddError_WhenRateLimitFieldDoesNotExist() {
        var mapWithoutRateLimit = new HashMap<String, Object>(Map.of("abc", 100));
        var validation = new ValidationException();
        RateLimitSettings.rejectRateLimitFieldForRequestContext(
            mapWithoutRateLimit,
            "scope",
            "service",
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.REQUEST,
            validation
        );
        assertTrue(validation.validationErrors().isEmpty());
    }

    public void testRejectRateLimitFieldForRequestContext_DoesNotAddError_WhenRateLimitFieldDoesExist_PersistentContext() {
        var mapWithRateLimit = new HashMap<String, Object>(
            Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100)))
        );
        var validation = new ValidationException();
        RateLimitSettings.rejectRateLimitFieldForRequestContext(
            mapWithRateLimit,
            "scope",
            "service",
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.PERSISTENT,
            validation
        );
        assertTrue(validation.validationErrors().isEmpty());
    }

    public void testRejectRateLimitFieldForRequestContext_DoesAddError_WhenRateLimitFieldDoesExist() {
        var mapWithRateLimit = new HashMap<String, Object>(
            Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100)))
        );
        var validation = new ValidationException();
        RateLimitSettings.rejectRateLimitFieldForRequestContext(
            mapWithRateLimit,
            "scope",
            "service",
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.REQUEST,
            validation
        );
        assertThat(
            validation.getMessage(),
            containsString("[scope] rate limit settings are not permitted for service [service] and task type [chat_completion]")
        );
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
        var requestsPerTimeUnit = instance.requestsPerTimeUnit();
        var timeUnit = instance.timeUnit();
        var enabled = instance.isEnabled();
        switch (randomInt(2)) {
            case 0 -> requestsPerTimeUnit = randomValueOtherThan(requestsPerTimeUnit, () -> randomLongBetween(1, 1000000));
            case 1 -> timeUnit = randomValueOtherThan(timeUnit, () -> randomFrom(TimeUnit.values()));
            case 2 -> enabled = enabled == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new RateLimitSettings(requestsPerTimeUnit, timeUnit, enabled);
    }

    @Override
    protected RateLimitSettings mutateInstanceForVersion(RateLimitSettings instance, TransportVersion version) {
        if (version.supports(INFERENCE_API_DISABLE_EIS_RATE_LIMITING) == false) {
            return new RateLimitSettings(instance.requestsPerTimeUnit(), instance.timeUnit(), true);
        } else {
            return instance;
        }
    }
}
