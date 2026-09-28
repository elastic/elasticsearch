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
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class UpdateServiceSettingsOPBuilderTests extends ESTestCase {

    private static final int TEST_REQUESTS_PER_MINUTE = 200;
    private static final String UNKNOWN_FIELD = "unknown_field";
    private static final String ACCESS_KEY_FIELD = "access_key";
    private static final String SECRET_KEY_FIELD = "secret_key";

    /**
     * Simple holder so tests can verify that the rate-limit setter was invoked and received the correct tri-state value.
     */
    private static class UpdateHolder {
        StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();

        void setRateLimitSettings(StatefulValue<RateLimitSettings> value) {
            this.rateLimitSettings = value;
        }
    }

    private UpdateHolder parse(ObjectParser<UpdateHolder, Void> parser, String json) throws IOException {
        try (var xParser = createParser(JsonXContent.jsonXContent, json)) {
            return parser.parse(xParser, null);
        }
    }

    public void testOf_RateLimitAbsent_LeavesUndefined() throws IOException {
        var parser = UpdateServiceSettingsOPBuilder.of(UpdateHolder::new, UpdateHolder::setRateLimitSettings).build();

        var holder = parse(parser, "{}");

        assertTrue(holder.rateLimitSettings.isUndefined());
        assertThat(holder.rateLimitSettings, sameInstance(StatefulValue.undefined()));
    }

    public void testOf_RateLimitExplicitNull_SetsNull() throws IOException {
        var parser = UpdateServiceSettingsOPBuilder.of(UpdateHolder::new, UpdateHolder::setRateLimitSettings).build();

        var holder = parse(parser, Strings.format("{\"%s\": null}", RateLimitSettings.FIELD_NAME));

        assertTrue(holder.rateLimitSettings.isNull());
    }

    public void testOf_RateLimitEmptyObject_SetsNull() throws IOException {
        // An empty {} for an OBJECT_OR_NULL field triggers the null-returning inner parser path.
        var parser = UpdateServiceSettingsOPBuilder.of(UpdateHolder::new, UpdateHolder::setRateLimitSettings).build();

        var holder = parse(parser, Strings.format("{\"%s\": {}}", RateLimitSettings.FIELD_NAME));

        assertTrue(holder.rateLimitSettings.isNull());
    }

    public void testOf_RateLimitValue_SetsPresent() throws IOException {
        var parser = UpdateServiceSettingsOPBuilder.of(UpdateHolder::new, UpdateHolder::setRateLimitSettings).build();

        var holder = parse(
            parser,
            Strings.format(
                "{\"%s\": {\"%s\": %d}}",
                RateLimitSettings.FIELD_NAME,
                RateLimitSettings.REQUESTS_PER_MINUTE_FIELD,
                TEST_REQUESTS_PER_MINUTE
            )
        );

        assertTrue(holder.rateLimitSettings.isPresent());
        assertThat(holder.rateLimitSettings.get(), is(new RateLimitSettings(TEST_REQUESTS_PER_MINUTE)));
    }

    public void testBuild_UnknownField_Throws() {
        var parser = new UpdateServiceSettingsOPBuilder<>(UpdateHolder::new).build();

        var ex = expectThrows(XContentParseException.class, () -> parse(parser, Strings.format("{\"%s\": 1}", UNKNOWN_FIELD)));

        assertThat(
            ex.getMessage(),
            endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, UNKNOWN_FIELD))
        );
    }

    public void testAllowApiKey_ApiKeyIsParsedAndIgnored() throws IOException {
        var parser = new UpdateServiceSettingsOPBuilder<>(UpdateHolder::new).allowApiKey().build();

        // Must not throw; api_key content is dropped
        var holder = parse(parser, Strings.format("{\"%s\": \"my-key\"}", DefaultSecretSettings.API_KEY));

        assertTrue(holder.rateLimitSettings.isUndefined());
    }

    public void testAllowSecretFields_MultipleFields_AreParsedAndIgnored() throws IOException {
        // Bedrock update uses access_key + secret_key
        var parser = new UpdateServiceSettingsOPBuilder<>(UpdateHolder::new).allowSecretFields(ACCESS_KEY_FIELD, SECRET_KEY_FIELD).build();

        var json = Strings.format("""
            {"%s": "acc", "%s": "sec"}
            """, ACCESS_KEY_FIELD, SECRET_KEY_FIELD);

        // Must not throw
        var holder = parse(parser, json);

        assertTrue(holder.rateLimitSettings.isUndefined());
    }

    public void testAllowSecretFields_NotDeclared_UnknownApiKey_Throws() {
        var parser = new UpdateServiceSettingsOPBuilder<>(UpdateHolder::new).build();

        var ex = expectThrows(
            XContentParseException.class,
            () -> parse(parser, Strings.format("{\"%s\": \"my-key\"}", DefaultSecretSettings.API_KEY))
        );

        assertThat(
            ex.getMessage(),
            endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, DefaultSecretSettings.API_KEY))
        );
    }

    public void testAllowApiKey_ExplicitNullValue_Throws() {
        // declareString uses ValueType.STRING which rejects VALUE_NULL — a plausible but incorrect
        // "clear api_key" request attempt should be rejected explicitly.
        var parser = new UpdateServiceSettingsOPBuilder<>(UpdateHolder::new).allowApiKey().build();

        expectThrows(XContentParseException.class, () -> parse(parser, Strings.format("{\"%s\": null}", DefaultSecretSettings.API_KEY)));
    }

    public void testAllowApiKeyTwice_Build_DeduplicatesField() throws IOException {
        // secretFields is a Set — duplicate allowApiKey() calls are silently ignored; build() must not throw.
        var parser = new UpdateServiceSettingsOPBuilder<>(UpdateHolder::new).allowApiKey().allowApiKey().build();

        // api_key is accepted once and its value dropped
        var holder = parse(parser, Strings.format("{\"%s\": \"my-key\"}", DefaultSecretSettings.API_KEY));

        assertTrue(holder.rateLimitSettings.isUndefined());
    }

    public void testOfThenAllowSecretFieldsWithApiKey_Deduplicates() throws IOException {
        // of(...) already calls allowApiKey(); passing API_KEY again via allowSecretFields() must not cause a duplicate declaration.
        var parser = UpdateServiceSettingsOPBuilder.of(UpdateHolder::new, UpdateHolder::setRateLimitSettings)
            .allowSecretFields(DefaultSecretSettings.API_KEY, ACCESS_KEY_FIELD)
            .build();

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

        var holder = parse(parser, json);

        assertTrue(holder.rateLimitSettings.isPresent());
        assertThat(holder.rateLimitSettings.get(), is(new RateLimitSettings(TEST_REQUESTS_PER_MINUTE)));
    }

    public void testAllowSecretFields_DuplicateNamesInSingleCall_Deduplicates() throws IOException {
        // Duplicates within a single varargs call are also absorbed by the set.
        var parser = new UpdateServiceSettingsOPBuilder<>(UpdateHolder::new).allowSecretFields(
            DefaultSecretSettings.API_KEY,
            DefaultSecretSettings.API_KEY
        ).build();

        var holder = parse(parser, Strings.format("{\"%s\": \"my-key\"}", DefaultSecretSettings.API_KEY));

        assertTrue(holder.rateLimitSettings.isUndefined());
    }

    public void testBuild_RateLimitSetterNotSet_RateLimitField_Throws() {
        // Without setRateLimitSettings(), rate_limit is undeclared and becomes an unknown field.
        var parser = new UpdateServiceSettingsOPBuilder<>(UpdateHolder::new).build();

        var ex = expectThrows(
            XContentParseException.class,
            () -> parse(parser, Strings.format("{\"%s\": {\"requests_per_minute\": 100}}", RateLimitSettings.FIELD_NAME))
        );

        assertThat(
            ex.getMessage(),
            endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, RateLimitSettings.FIELD_NAME))
        );
    }

    public void testConstructor_NullValueSupplier_ThrowsNullPointerException() {
        expectThrows(NullPointerException.class, () -> new UpdateServiceSettingsOPBuilder<UpdateHolder>(null));
    }

    public void testOf_NullValueSupplier_ThrowsNullPointerException() {
        expectThrows(NullPointerException.class, () -> UpdateServiceSettingsOPBuilder.of(null, UpdateHolder::setRateLimitSettings));
    }
}
