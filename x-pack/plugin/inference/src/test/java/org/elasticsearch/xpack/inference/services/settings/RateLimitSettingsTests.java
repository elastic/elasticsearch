/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class RateLimitSettingsTests extends AbstractBWCWireSerializationTestCase<RateLimitSettings> {

    private static final TransportVersion INFERENCE_API_DISABLE_EIS_RATE_LIMITING = TransportVersion.fromName(
        "inference_api_disable_eis_rate_limiting"
    );
    private static final String TEST_SERVICE_NAME = "test-service";
    private static final String TEST_SCOPE = "some-scope";
    private static final int TEST_REQUESTS_PER_MINUTE = 100;
    private static final String TEST_UNKNOWN_FIELD_NAME = "some-unknown-field";

    public static RateLimitSettings createRandom() {
        return new RateLimitSettings(randomLongBetween(1, 1000000));
    }

    /**
     * Helper method to add rate limit settings to service settings
     */
    public static Map<String, Object> addRateLimitSettingsToMap(Map<String, Object> settingsMap, long requestsPerMinute) {
        settingsMap.put(
            RateLimitSettings.FIELD_NAME,
            new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, requestsPerMinute))
        );
        return settingsMap;
    }

    public void testConstructor_ZeroValue_ThrowsIllegalArgumentException() {
        var zeroValue = 0L;
        assertConstructor_InvalidValue_ThrowsIllegalArgumentException(() -> new RateLimitSettings(zeroValue));
    }

    public void testConstructor_NegativeValue_ThrowsIllegalArgumentException() {
        var negativeValue = randomNegativeLong();
        assertConstructor_InvalidValue_ThrowsIllegalArgumentException(() -> new RateLimitSettings(negativeValue));
    }

    public void testConstructor_ZeroValueWithTimeUnit_ThrowsIllegalArgumentException() {
        var zeroValue = 0L;
        assertConstructor_InvalidValue_ThrowsIllegalArgumentException(() -> new RateLimitSettings(zeroValue, TimeUnit.MILLISECONDS));
    }

    public void testConstructor_NegativeValueWithTimeUnit_ThrowsIllegalArgumentException() {
        long negativeValue = randomNegativeLong();
        assertConstructor_InvalidValue_ThrowsIllegalArgumentException(() -> new RateLimitSettings(negativeValue, TimeUnit.MILLISECONDS));
    }

    private static void assertConstructor_InvalidValue_ThrowsIllegalArgumentException(ThrowingRunnable throwingRunnable) {
        var exception = expectThrows(IllegalArgumentException.class, throwingRunnable);
        assertThat(exception.getMessage(), is("requests per minute must be positive"));
    }

    public void testOf_RequestPerMinutePresent_CreatesInstanceSuccessfully() {
        var validationException = new ValidationException();
        Map<String, Object> settingsMap = new HashMap<>(
            Map.of(
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_REQUESTS_PER_MINUTE))
            )
        );

        var settings = RateLimitSettings.of(
            settingsMap,
            createRandom(),
            validationException,
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(settings, is(new RateLimitSettings(TEST_REQUESTS_PER_MINUTE)));
        assertTrue(settings.isEnabled());
        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testOf_UsesDefaultValue_WhenRateLimit_IsAbsent() {
        var validationException = new ValidationException();
        Map<String, Object> settingsMap = new HashMap<>(
            Map.of(TEST_UNKNOWN_FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_REQUESTS_PER_MINUTE)))
        );
        var defaultValue = createRandom();

        var settings = RateLimitSettings.of(settingsMap, defaultValue, validationException, randomFrom(ConfigurationParseContext.values()));

        assertThat(settings, sameInstance(defaultValue));
        assertTrue(settings.isEnabled());
        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testOf_NoRequestsPerMinute_UsesDefaultValue() {
        var validationException = new ValidationException();
        Map<String, Object> settingsMap = new HashMap<>(Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>()));
        var defaultValue = createRandom();

        var settings = RateLimitSettings.of(settingsMap, defaultValue, validationException, randomFrom(ConfigurationParseContext.values()));

        assertThat(settings, sameInstance(defaultValue));
        assertTrue(settings.isEnabled());
        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testCreateParser_RequestsPerMinutePresent_ReturnsParsedValue() throws IOException {
        var settings = parseRateLimit(
            Strings.format("{\"%s\": %d}", RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_REQUESTS_PER_MINUTE),
            randomBoolean(),
            createRandom()
        );

        assertThat(settings, is(new RateLimitSettings(TEST_REQUESTS_PER_MINUTE)));
    }

    public void testCreateParser_EmptyObject_ReturnsDefaultValue() throws IOException {
        var defaultValue = createRandom();

        var settings = parseRateLimit("{}", randomBoolean(), defaultValue);

        assertThat(settings, sameInstance(defaultValue));
    }

    public void testCreateParser_EmptyObject_NullDefault_ReturnsNull() throws IOException {
        var settings = parseRateLimit("{}", randomBoolean(), null);

        assertThat(settings, is(nullValue()));
    }

    private RateLimitSettings parseRateLimit(String json, boolean ignoreUnknownFields, @Nullable RateLimitSettings defaultValue)
        throws IOException {
        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            return RateLimitSettings.createParser(ignoreUnknownFields, defaultValue).apply(parser, null);
        }
    }

    public void testOf_ZeroValue_AddsValidationError() {
        var zeroValue = 0L;
        assertOf_InvalidValue_AddsValidationError(zeroValue);
    }

    public void testOf_NegativeValue_AddsValidationError() {
        var negativeValue = randomNegativeLong();
        assertOf_InvalidValue_AddsValidationError(negativeValue);
    }

    private static void assertOf_InvalidValue_AddsValidationError(long invalidValue) {
        var validationException = new ValidationException();
        Map<String, Object> settingsMap = new HashMap<>(
            Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, invalidValue)))
        );
        var defaultValue = createRandom();

        var settings = RateLimitSettings.of(settingsMap, defaultValue, validationException, randomFrom(ConfigurationParseContext.values()));

        assertThat(settings, sameInstance(defaultValue));
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(
            validationException.validationErrors().getFirst(),
            is(
                Strings.format(
                    "[%s] Invalid value [%s]. [%s] must be a positive long",
                    RateLimitSettings.FIELD_NAME,
                    invalidValue,
                    RateLimitSettings.REQUESTS_PER_MINUTE_FIELD
                )
            )
        );
    }

    public void testOf_RequestContext_WithUnknownField_AddsValidationError() {
        var validationException = new ValidationException();
        Map<String, Object> settingsMap = new HashMap<>(
            Map.of(
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(
                    Map.of(
                        RateLimitSettings.REQUESTS_PER_MINUTE_FIELD,
                        TEST_REQUESTS_PER_MINUTE,
                        TEST_UNKNOWN_FIELD_NAME,
                        TEST_REQUESTS_PER_MINUTE
                    )
                )
            )
        );
        var defaultValue = createRandom();

        var settings = RateLimitSettings.of(settingsMap, defaultValue, validationException, ConfigurationParseContext.REQUEST);

        assertThat(settings, sameInstance(defaultValue));
        assertTrue(settings.isEnabled());
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(
            validationException.validationErrors().getFirst(),
            is(Strings.format("Rate limit settings contain unknown entries [{%s=%d}]", TEST_UNKNOWN_FIELD_NAME, TEST_REQUESTS_PER_MINUTE))
        );
    }

    public void testOf_PersistentContext_WithUnknownField_DoesNotAddValidationError() {
        var validationException = new ValidationException();
        Map<String, Object> settingsMap = new HashMap<>(
            Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(TEST_UNKNOWN_FIELD_NAME, TEST_REQUESTS_PER_MINUTE)))
        );
        var defaultValue = createRandom();

        var settings = RateLimitSettings.of(settingsMap, defaultValue, validationException, ConfigurationParseContext.PERSISTENT);

        assertThat(settings, sameInstance(defaultValue));
        assertTrue(settings.isEnabled());
        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testToXContent_HasRequestsPerMinuteValue_WritesSuccessfully() throws IOException {
        var settings = new RateLimitSettings(TEST_REQUESTS_PER_MINUTE);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        settings.toXContent(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }""", TEST_REQUESTS_PER_MINUTE))));
    }

    public void testToXContent_EnabledIsFalse_IsDisabledAndNotSerialized() throws IOException {
        var settings = new RateLimitSettings(TEST_REQUESTS_PER_MINUTE, TimeUnit.MINUTES, false);

        assertToXContent_IsDisabledAndNotSerialized(settings);
    }

    public void testToXContent_DisabledInstance_IsDisabledAndNotSerialized() throws IOException {
        var settings = RateLimitSettings.DISABLED_INSTANCE;

        assertToXContent_IsDisabledAndNotSerialized(settings);
    }

    private static void assertToXContent_IsDisabledAndNotSerialized(RateLimitSettings settings) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        settings.toXContent(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        assertThat(settings.isEnabled(), is(false));
        assertThat(xContentResult, is("{}"));
    }

    public void testRejectRateLimitFieldForRequestContext_RequestContext_NoRateLimitField_DoesNotAddValidationError() {
        var mapWithoutRateLimit = new HashMap<String, Object>(Map.of(TEST_UNKNOWN_FIELD_NAME, TEST_REQUESTS_PER_MINUTE));
        var validationException = new ValidationException();

        RateLimitSettings.rejectRateLimitFieldForRequestContext(
            mapWithoutRateLimit,
            TEST_SCOPE,
            TEST_SERVICE_NAME,
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.REQUEST,
            validationException
        );

        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testRejectRateLimitFieldForRequestContext_PersistentContext_WithRateLimitField_DoesNotAddValidationError() {
        var mapWithRateLimit = new HashMap<String, Object>(
            Map.of(
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_REQUESTS_PER_MINUTE))
            )
        );
        var validationException = new ValidationException();

        RateLimitSettings.rejectRateLimitFieldForRequestContext(
            mapWithRateLimit,
            TEST_SCOPE,
            TEST_SERVICE_NAME,
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.PERSISTENT,
            validationException
        );

        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testRejectRateLimitFieldForRequestContext_RequestContext_WithRateLimitField_AddsValidationError() {
        var mapWithRateLimit = new HashMap<String, Object>(
            Map.of(
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_REQUESTS_PER_MINUTE))
            )
        );
        var validationException = new ValidationException();
        var taskType = randomFrom(TaskType.values());

        RateLimitSettings.rejectRateLimitFieldForRequestContext(
            mapWithRateLimit,
            TEST_SCOPE,
            TEST_SERVICE_NAME,
            taskType,
            ConfigurationParseContext.REQUEST,
            validationException
        );

        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(
            validationException.validationErrors().getFirst(),
            is(
                Strings.format(
                    "[%s] rate limit settings are not permitted for service [%s] and task type [%s]",
                    TEST_SCOPE,
                    TEST_SERVICE_NAME,
                    taskType.toString()
                )
            )
        );
    }

    public void testDeclareUnsupportedRateLimitField_RequestContext_ThrowsElasticsearchParseException() {
        var taskType = randomFrom(TaskType.values());
        var json = Strings.format("""
            {"%s": {"%s": %d}}""", RateLimitSettings.FIELD_NAME, RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_REQUESTS_PER_MINUTE);

        var exception = expectThrows(
            XContentParseException.class,
            () -> parseWithRejectDeclare(json, taskType, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getCause(), instanceOf(ElasticsearchParseException.class));
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                Strings.format(
                    "[%s] rate limit settings are not permitted for service [%s] and task type [%s]",
                    TEST_SCOPE,
                    TEST_SERVICE_NAME,
                    taskType.toString()
                )
            )
        );
    }

    public void testDeclareUnsupportedRateLimitField_RequestContext_NoRateLimitField_DoesNotThrow() throws IOException {
        var holder = parseWithRejectDeclare("{}", randomFrom(TaskType.values()), ConfigurationParseContext.REQUEST);

        assertThat(holder.rateLimitSettings, nullValue());
    }

    public void testDeclareUnsupportedRateLimitField_PersistentContext_IgnoresRateLimitField() throws IOException {
        var json = Strings.format("""
            {"%s": {"%s": %d}}""", RateLimitSettings.FIELD_NAME, RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_REQUESTS_PER_MINUTE);

        var holder = parseWithRejectDeclare(json, randomFrom(TaskType.values()), ConfigurationParseContext.PERSISTENT);

        assertThat(holder.rateLimitSettings, nullValue());
    }

    private RateLimitHolder parseWithRejectDeclare(String json, TaskType taskType, ConfigurationParseContext ctx) throws IOException {
        // Mirrors production usage: the request parser is strict, the persistent parser tolerates unknown fields.
        var parser = new ObjectParser<RateLimitHolder, ConfigurationParseContext>(
            "test",
            ctx == ConfigurationParseContext.PERSISTENT,
            RateLimitHolder::new
        );
        RateLimitSettings.declareUnsupportedRateLimitField(parser, TEST_SCOPE, TEST_SERVICE_NAME, taskType, ctx);
        try (var xParser = createParser(JsonXContent.jsonXContent, json)) {
            return parser.parse(xParser, ctx);
        }
    }

    private static class RateLimitHolder {
        private RateLimitSettings rateLimitSettings;

        private void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }
    }

    private RateLimitHolder parseWithDeclare(String json, RateLimitSettings defaultValue, ConfigurationParseContext ctx)
        throws IOException {
        var parser = new ObjectParser<RateLimitHolder, ConfigurationParseContext>("test", RateLimitHolder::new);
        RateLimitSettings.declareRateLimitSettings(parser, RateLimitHolder::setRateLimitSettings, defaultValue);
        try (var xParser = createParser(JsonXContent.jsonXContent, json)) {
            return parser.parse(xParser, ctx);
        }
    }

    public void testDeclareRateLimitSettings_RequestsPerMinutePresent_SetsParsedValue() throws IOException {
        var holder = parseWithDeclare(
            Strings.format("""
                    {
                        "%s": {
                            "%s": %d
                        }
                    }
                """, RateLimitSettings.FIELD_NAME, RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_REQUESTS_PER_MINUTE),
            createRandom(),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(holder.rateLimitSettings, is(new RateLimitSettings(TEST_REQUESTS_PER_MINUTE)));
    }

    public void testDeclareRateLimitSettings_EmptyObject_SetsDefaultValueNotNull() throws IOException {
        var defaultValue = createRandom();
        var holder = parseWithDeclare(Strings.format("""
                {
                    "%s": {}
                }
            """, RateLimitSettings.FIELD_NAME), defaultValue, randomFrom(ConfigurationParseContext.values()));

        assertThat(holder.rateLimitSettings, sameInstance(defaultValue));
    }

    public void testDeclareRateLimitSettings_Absent_DoesNotInvokeSetter() throws IOException {
        var holder = parseWithDeclare("{}", createRandom(), randomFrom(ConfigurationParseContext.values()));

        assertNull(holder.rateLimitSettings);
    }

    public void testDeclareRateLimitSettings_PersistentContext_IgnoresUnknownField() throws IOException {
        var holder = parseWithDeclare(
            Strings.format(
                """
                        {
                            "%s": {
                                "%s": %d,
                                "%s": 1
                            }
                        }
                    """,
                RateLimitSettings.FIELD_NAME,
                RateLimitSettings.REQUESTS_PER_MINUTE_FIELD,
                TEST_REQUESTS_PER_MINUTE,
                TEST_UNKNOWN_FIELD_NAME
            ),
            createRandom(),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(holder.rateLimitSettings, is(new RateLimitSettings(TEST_REQUESTS_PER_MINUTE)));
    }

    public void testDeclareRateLimitSettings_RequestContext_UnknownField_Throws() {
        var ex = expectThrows(
            XContentParseException.class,
            () -> parseWithDeclare(
                Strings.format(
                    """
                            {
                                "%s": {
                                    "%s": %d,
                                    "%s": 1
                                }
                            }
                        """,
                    RateLimitSettings.FIELD_NAME,
                    RateLimitSettings.REQUESTS_PER_MINUTE_FIELD,
                    TEST_REQUESTS_PER_MINUTE,
                    TEST_UNKNOWN_FIELD_NAME
                ),
                createRandom(),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(ex.getMessage(), containsString(Strings.format("failed to parse field [%s]", RateLimitSettings.FIELD_NAME)));

        assertThat(
            ex.getCause().getMessage(),
            containsString(Strings.format("[%s] unknown field [%s]", RateLimitSettings.FIELD_NAME, TEST_UNKNOWN_FIELD_NAME))
        );
    }

    private static class UpdateHolder {
        private StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();

        private void setRateLimitSettings(StatefulValue<RateLimitSettings> rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }
    }

    private UpdateHolder parseWithDeclareUpdatable(String json) throws IOException {
        var parser = new ObjectParser<UpdateHolder, Void>("test", UpdateHolder::new);
        RateLimitSettings.declareUpdatableRateLimitSettings(parser, UpdateHolder::setRateLimitSettings);
        try (var xParser = createParser(JsonXContent.jsonXContent, json)) {
            return parser.parse(xParser, null);
        }
    }

    public void testDeclareUpdatableRateLimitSettings_ValuePresent_SetsPresentValue() throws IOException {
        var holder = parseWithDeclareUpdatable(Strings.format("""
                {
                    "%s": {
                        "%s": %d
                    }
                }
            """, RateLimitSettings.FIELD_NAME, RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_REQUESTS_PER_MINUTE));

        assertTrue(holder.rateLimitSettings.isPresent());
        assertThat(holder.rateLimitSettings.get(), is(new RateLimitSettings(TEST_REQUESTS_PER_MINUTE)));
    }

    public void testDeclareUpdatableRateLimitSettings_ExplicitNull_SetsNull() throws IOException {
        var holder = parseWithDeclareUpdatable(Strings.format("""
                {
                    "%s": null
                }
            """, RateLimitSettings.FIELD_NAME));

        assertTrue(holder.rateLimitSettings.isNull());
    }

    public void testDeclareUpdatableRateLimitSettings_EmptyObject_SetsNull() throws IOException {
        var holder = parseWithDeclareUpdatable(Strings.format("""
                {
                    "%s": {}
                }
            """, RateLimitSettings.FIELD_NAME));

        assertTrue(holder.rateLimitSettings.isNull());
    }

    public void testDeclareUpdatableRateLimitSettings_Absent_LeavesUndefined() throws IOException {
        var holder = parseWithDeclareUpdatable("{}");

        assertTrue(holder.rateLimitSettings.isUndefined());
        assertThat(holder.rateLimitSettings, sameInstance(StatefulValue.undefined()));
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
