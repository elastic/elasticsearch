/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

public class ServiceSettingsOPBuilderTests extends ESTestCase {

    private static final int TEST_REQUESTS_PER_MINUTE = 200;
    private static final RateLimitSettings TEST_DEFAULT_RATE_LIMIT = new RateLimitSettings(100);
    private static final String UNKNOWN_FIELD = "unknown_field";
    private static final String ACCESS_KEY_FIELD = "access_key";
    private static final String SECRET_KEY_FIELD = "secret_key";

    /**
     * Simple holder so tests can verify that the rate-limit setter was invoked and received the correct value.
     */
    private static class RateLimitHolder {
        RateLimitSettings rateLimitSettings;

        void setRateLimitSettings(RateLimitSettings value) {
            this.rateLimitSettings = value;
        }
    }

    private RateLimitHolder parse(
        ObjectParser<RateLimitHolder, ConfigurationParseContext> parser,
        String json,
        ConfigurationParseContext ctx
    ) throws IOException {
        try (var xParser = createParser(JsonXContent.jsonXContent, json)) {
            return parser.parse(xParser, ctx);
        }
    }

    private RateLimitHolder parse(
        ObjectParser<RateLimitHolder, ConfigurationParseContext> parser,
        String json,
        RateLimitHolder value,
        ConfigurationParseContext ctx
    ) throws IOException {
        try (var xParser = createParser(JsonXContent.jsonXContent, json)) {
            return parser.parse(xParser, value, ctx);
        }
    }

    public void testOf_DeclaresRateLimitAndApiKey_BothAccepted() throws IOException {
        var parser = ServiceSettingsOPBuilder.of(
            false,
            RateLimitHolder::new,
            TEST_DEFAULT_RATE_LIMIT,
            RateLimitHolder::setRateLimitSettings
        ).build();

        var json = Strings.format(
            """
                {
                  "%s": {"%s": %d},
                  "%s": "my-key"
                }
                """,
            RateLimitSettings.FIELD_NAME,
            RateLimitSettings.REQUESTS_PER_MINUTE_FIELD,
            TEST_REQUESTS_PER_MINUTE,
            DefaultSecretSettings.API_KEY
        );

        var holder = parse(parser, json, ConfigurationParseContext.REQUEST);

        assertThat(holder.rateLimitSettings, is(new RateLimitSettings(TEST_REQUESTS_PER_MINUTE)));
    }

    public void testOf_RateLimitAbsent_UsesDefault() throws IOException {
        var parser = ServiceSettingsOPBuilder.of(
            false,
            RateLimitHolder::new,
            TEST_DEFAULT_RATE_LIMIT,
            RateLimitHolder::setRateLimitSettings
        ).build();

        // no rate_limit field → setter is not invoked → field stays null, so the outer
        // service settings class falls back to its default; here we simply check the
        // setter was NOT called (holder field stays null).
        var holder = parse(parser, "{}", ConfigurationParseContext.REQUEST);

        assertNull(holder.rateLimitSettings);
    }

    public void testBuild_IgnoreUnknownFieldsFalse_UnknownField_Throws() {
        var parser = new ServiceSettingsOPBuilder<>(false, RateLimitHolder::new).build();

        var ex = expectThrows(
            XContentParseException.class,
            () -> parse(parser, Strings.format("{\"%s\": 1}", UNKNOWN_FIELD), ConfigurationParseContext.REQUEST)
        );

        assertThat(
            ex.getMessage(),
            endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, UNKNOWN_FIELD))
        );
    }

    public void testBuild_IgnoreUnknownFieldsTrue_UnknownField_IsIgnored() throws IOException {
        var parser = new ServiceSettingsOPBuilder<>(true, RateLimitHolder::new).build();

        // Must not throw
        parse(parser, Strings.format("{\"%s\": 1}", UNKNOWN_FIELD), ConfigurationParseContext.PERSISTENT);
    }

    public void testBuild_IgnoreUnknownFieldsTrue_RequestContext_UnknownFieldInsideRateLimit_Throws() {
        var parser = new ServiceSettingsOPBuilder<>(true, RateLimitHolder::new).enableRateLimitSettings(
            RateLimitHolder::setRateLimitSettings,
            TEST_DEFAULT_RATE_LIMIT
        ).build();

        var json = Strings.format("""
            {"%s": {"%s": %d, "%s": 1}}
            """, RateLimitSettings.FIELD_NAME, RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_REQUESTS_PER_MINUTE, UNKNOWN_FIELD);

        var ex = expectThrows(XContentParseException.class, () -> parse(parser, json, ConfigurationParseContext.REQUEST));

        assertThat(ex.getMessage(), containsString(Strings.format("failed to parse field [%s]", RateLimitSettings.FIELD_NAME)));
        assertThat(
            ex.getCause().getMessage(),
            containsString(Strings.format("[%s] unknown field [%s]", RateLimitSettings.FIELD_NAME, UNKNOWN_FIELD))
        );
    }

    public void testBuild_IgnoreUnknownFieldsFalse_PersistentContext_UnknownFieldInsideRateLimit_IsIgnored() throws IOException {
        // Outer ignoreUnknownFields=false, but PERSISTENT context makes the rate_limit inner parser lenient.
        var parser = new ServiceSettingsOPBuilder<>(false, RateLimitHolder::new).enableRateLimitSettings(
            RateLimitHolder::setRateLimitSettings,
            TEST_DEFAULT_RATE_LIMIT
        ).build();

        var json = Strings.format("""
            {"%s": {"%s": %d, "%s": 1}}
            """, RateLimitSettings.FIELD_NAME, RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_REQUESTS_PER_MINUTE, UNKNOWN_FIELD);

        // Must not throw
        var holder = parse(parser, json, ConfigurationParseContext.PERSISTENT);

        assertThat(holder.rateLimitSettings, is(new RateLimitSettings(TEST_REQUESTS_PER_MINUTE)));
    }

    public void testAllowApiKey_ApiKeyIsParsedAndIgnored() throws IOException {
        var parser = new ServiceSettingsOPBuilder<>(false, RateLimitHolder::new).allowApiKey().build();

        // Must not throw; api_key content is dropped
        var holder = parse(
            parser,
            Strings.format("{\"%s\": \"my-key\"}", DefaultSecretSettings.API_KEY),
            ConfigurationParseContext.REQUEST
        );

        assertNull(holder.rateLimitSettings);
    }

    public void testAllowSecretFields_MultipleFields_AreParsedAndIgnored() throws IOException {
        var parser = new ServiceSettingsOPBuilder<>(false, RateLimitHolder::new).allowSecretFields(ACCESS_KEY_FIELD, SECRET_KEY_FIELD)
            .build();

        var json = Strings.format("""
            {"%s": "acc", "%s": "sec"}
            """, ACCESS_KEY_FIELD, SECRET_KEY_FIELD);

        // Must not throw
        var holder = parse(parser, json, ConfigurationParseContext.REQUEST);

        assertNull(holder.rateLimitSettings);
    }

    public void testAllowSecretFields_NotDeclared_UnknownApiKey_Throws() {
        var parser = new ServiceSettingsOPBuilder<>(false, RateLimitHolder::new).build();

        var ex = expectThrows(
            XContentParseException.class,
            () -> parse(parser, Strings.format("{\"%s\": \"my-key\"}", DefaultSecretSettings.API_KEY), ConfigurationParseContext.REQUEST)
        );

        assertThat(
            ex.getMessage(),
            endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, DefaultSecretSettings.API_KEY))
        );
    }

    public void testAllowSecretFields_EmptyVarargs_DeclaresNothing() throws IOException {
        // allowSecretFields() with no args is a no-op; subsequent unknown fields still throw
        var parser = new ServiceSettingsOPBuilder<>(false, RateLimitHolder::new).allowSecretFields().build();

        var ex = expectThrows(
            XContentParseException.class,
            () -> parse(parser, Strings.format("{\"%s\": \"my-key\"}", DefaultSecretSettings.API_KEY), ConfigurationParseContext.REQUEST)
        );

        assertThat(
            ex.getMessage(),
            endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, DefaultSecretSettings.API_KEY))
        );
    }

    public void testAllowApiKey_ExplicitNullValue_Throws() {
        // declareString uses ValueType.STRING which rejects VALUE_NULL.
        var parser = new ServiceSettingsOPBuilder<>(false, RateLimitHolder::new).allowApiKey().build();

        expectThrows(
            XContentParseException.class,
            () -> parse(parser, Strings.format("{\"%s\": null}", DefaultSecretSettings.API_KEY), ConfigurationParseContext.REQUEST)
        );
    }

    public void testAllowApiKeyTwice_Build_DeduplicatesField() throws IOException {
        // secretFields is a Set — duplicate allowApiKey() calls are silently ignored; build() must not throw.
        var parser = new ServiceSettingsOPBuilder<>(false, RateLimitHolder::new).allowApiKey().allowApiKey().build();

        // api_key is accepted once and its value dropped
        var holder = parse(
            parser,
            Strings.format("{\"%s\": \"my-key\"}", DefaultSecretSettings.API_KEY),
            ConfigurationParseContext.REQUEST
        );

        assertNull(holder.rateLimitSettings);
    }

    public void testOfThenAllowSecretFieldsWithApiKey_Deduplicates() throws IOException {
        // of(...) already calls allowApiKey(); passing API_KEY again via allowSecretFields() must not cause a duplicate declaration.
        var parser = ServiceSettingsOPBuilder.of(
            false,
            RateLimitHolder::new,
            TEST_DEFAULT_RATE_LIMIT,
            RateLimitHolder::setRateLimitSettings
        ).allowSecretFields(DefaultSecretSettings.API_KEY, ACCESS_KEY_FIELD).build();

        var json = Strings.format(
            """
                {
                  "%s": {"%s": %d},
                  "%s": "my-key",
                  "%s": "acc"
                }
                """,
            RateLimitSettings.FIELD_NAME,
            RateLimitSettings.REQUESTS_PER_MINUTE_FIELD,
            TEST_REQUESTS_PER_MINUTE,
            DefaultSecretSettings.API_KEY,
            ACCESS_KEY_FIELD
        );

        var holder = parse(parser, json, ConfigurationParseContext.REQUEST);

        assertThat(holder.rateLimitSettings, is(new RateLimitSettings(TEST_REQUESTS_PER_MINUTE)));
    }

    public void testAllowSecretFields_DuplicateNamesInSingleCall_Deduplicates() throws IOException {
        // Duplicates within a single varargs call are also absorbed by the set.
        var parser = new ServiceSettingsOPBuilder<>(false, RateLimitHolder::new).allowSecretFields(
            DefaultSecretSettings.API_KEY,
            DefaultSecretSettings.API_KEY
        ).build();

        var holder = parse(
            parser,
            Strings.format("{\"%s\": \"my-key\"}", DefaultSecretSettings.API_KEY),
            ConfigurationParseContext.REQUEST
        );

        assertNull(holder.rateLimitSettings);
    }

    public void testBuild_RateLimitNotEnabled_RateLimitField_Throws() {
        // Without enableRateLimitSettings(), rate_limit is undeclared and becomes an unknown field.
        var parser = new ServiceSettingsOPBuilder<>(false, RateLimitHolder::new).build();

        var ex = expectThrows(
            XContentParseException.class,
            () -> parse(
                parser,
                Strings.format("{\"%s\": {\"requests_per_minute\": 100}}", RateLimitSettings.FIELD_NAME),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(
            ex.getMessage(),
            endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, RateLimitSettings.FIELD_NAME))
        );
    }

    public void testConstructor_NullValueSupplier_ThrowsNullPointerException() {
        expectThrows(NullPointerException.class, () -> new ServiceSettingsOPBuilder<RateLimitHolder>(randomBoolean(), null));
    }

    public void testOf_NullValueSupplier_ThrowsNullPointerException() {
        expectThrows(
            NullPointerException.class,
            () -> ServiceSettingsOPBuilder.of(randomBoolean(), null, TEST_DEFAULT_RATE_LIMIT, RateLimitHolder::setRateLimitSettings)
        );
    }

    public void testBuild_ParseWithProvidedValue_UsesProvidedInstance() throws IOException {
        var parser = ServiceSettingsOPBuilder.of(
            false,
            RateLimitHolder::new,
            TEST_DEFAULT_RATE_LIMIT,
            RateLimitHolder::setRateLimitSettings
        ).build();

        var value = new RateLimitHolder();
        // The 3-arg overload uses the caller-provided instance, not the supplier's product.
        var result = parse(parser, "{}", value, ConfigurationParseContext.REQUEST);

        assertSame(value, result);
    }
}
