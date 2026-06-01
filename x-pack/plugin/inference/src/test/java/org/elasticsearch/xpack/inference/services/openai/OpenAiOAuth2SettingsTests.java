/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.AUTH_URL;
import static org.hamcrest.Matchers.is;

public class OpenAiOAuth2SettingsTests extends AbstractBWCWireSerializationTestCase<OpenAiOAuth2Settings> {

    public static final URI TEST_AUTH_URL = URI.create("https://idp.example.com/oauth2/token");
    public static final URI INITIAL_TEST_AUTH_URL = URI.create("https://initial-idp.example.com/oauth2/token");

    private static final String TEST_CLIENT_ID = "client-id";
    private static final List<String> TEST_SCOPES = List.of("scope-a", "scope-b");

    public static OpenAiOAuth2Settings createRandom() {
        return new OpenAiOAuth2Settings(
            randomAlphaOfLength(10),
            randomList(1, 3, () -> randomAlphaOfLength(5)),
            URI.create("https://" + randomAlphaOfLength(8) + ".example.com/token")
        );
    }

    @Override
    protected Writeable.Reader<OpenAiOAuth2Settings> instanceReader() {
        return OpenAiOAuth2Settings::new;
    }

    @Override
    protected OpenAiOAuth2Settings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenAiOAuth2Settings mutateInstance(OpenAiOAuth2Settings instance) throws IOException {
        return randomValueOtherThan(instance, OpenAiOAuth2SettingsTests::createRandom);
    }

    @Override
    protected OpenAiOAuth2Settings mutateInstanceForVersion(OpenAiOAuth2Settings instance, TransportVersion version) {
        return instance;
    }

    public void testFromMap_BuildsSettingsWhenAllFieldsPresent() {
        var map = new HashMap<String, Object>();
        map.put(OAuth2Settings.CLIENT_ID_FIELD, TEST_CLIENT_ID);
        map.put(OAuth2Settings.SCOPES_FIELD, TEST_SCOPES);
        map.put(AUTH_URL, TEST_AUTH_URL.toString());

        var validationException = new ValidationException();
        var result = OpenAiOAuth2Settings.fromMap(map, validationException);

        assertNotNull(result);
        assertThat(result.clientId(), is(TEST_CLIENT_ID));
        assertThat(result.scopes(), is(TEST_SCOPES));
        assertThat(result.authUrl(), is(TEST_AUTH_URL));
        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testFromMap_ReturnsNullAndNoErrorsWhenAllFieldsAbsent() {
        var map = new HashMap<String, Object>();
        var validationException = new ValidationException();

        var result = OpenAiOAuth2Settings.fromMap(map, validationException);

        assertNull(result);
        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testFromMap_ValidationErrorWhenAuthUrlMissing() {
        var map = new HashMap<String, Object>();
        map.put(OAuth2Settings.CLIENT_ID_FIELD, TEST_CLIENT_ID);
        map.put(OAuth2Settings.SCOPES_FIELD, TEST_SCOPES);

        var validationException = new ValidationException();
        var result = OpenAiOAuth2Settings.fromMap(map, validationException);

        assertNull(result);
        assertFalse(validationException.validationErrors().isEmpty());
        assertThat(
            validationException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] all OpenAI OAuth2 fields must be provided together; missing: [%s]", AUTH_URL))
        );
    }

    public void testFromMap_ValidationErrorWhenClientIdMissingButAuthUrlPresent() {
        var map = new HashMap<String, Object>();
        map.put(AUTH_URL, TEST_AUTH_URL.toString());

        var validationException = new ValidationException();
        var result = OpenAiOAuth2Settings.fromMap(map, validationException);

        assertNull(result);
        assertThat(
            validationException.validationErrors(),
            is(
                List.of(
                    Strings.format(
                        "[service_settings] all OpenAI OAuth2 fields must be provided together; missing: [%s, %s]",
                        OAuth2Settings.CLIENT_ID_FIELD,
                        OAuth2Settings.SCOPES_FIELD
                    )
                )
            )
        );
    }

    public void testUpdateServiceSettings_RetainsExistingFieldsWhenMapEmpty() {
        var original = new OpenAiOAuth2Settings(TEST_CLIENT_ID, TEST_SCOPES, TEST_AUTH_URL);
        var validationException = new ValidationException();

        var updated = original.updateServiceSettings(new HashMap<>(), validationException);

        assertThat(updated, is(original));
        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testUpdateServiceSettings_OverridesAuthUrl() {
        var original = new OpenAiOAuth2Settings(TEST_CLIENT_ID, TEST_SCOPES, INITIAL_TEST_AUTH_URL);

        var map = new HashMap<String, Object>();
        map.put(AUTH_URL, TEST_AUTH_URL.toString());

        var updated = original.updateServiceSettings(map, new ValidationException());

        assertThat(updated.authUrl(), is(TEST_AUTH_URL));
        assertThat(updated.clientId(), is(original.clientId()));
        assertThat(updated.scopes(), is(original.scopes()));
    }

    public void testUpdateServiceSettingsIfPresent_ReturnsNullWithNoErrorsWhenNoOAuth2Fields() {
        var validationException = new ValidationException();

        var result = OpenAiOAuth2Settings.updateServiceSettingsIfPresent(null, new HashMap<>(), validationException);

        assertNull(result);
        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testUpdateServiceSettingsIfPresent_AddsErrorWhenCurrentSettingsNullAndAuthUrlPresent() {
        var map = new HashMap<String, Object>();
        map.put(AUTH_URL, TEST_AUTH_URL.toString());

        var validationException = new ValidationException();

        var result = OpenAiOAuth2Settings.updateServiceSettingsIfPresent(null, map, validationException);

        assertNull(result);
        assertThat(validationException.validationErrors(), is(List.of(OAuth2Settings.OAUTH2_SETTINGS_NOT_CONFIGURED_ERROR)));
    }

    public void testUpdateServiceSettingsIfPresent_AddsErrorWhenCurrentSettingsNullAndClientIdPresent() {
        var map = new HashMap<String, Object>();
        map.put(OAuth2Settings.CLIENT_ID_FIELD, TEST_CLIENT_ID);

        var validationException = new ValidationException();

        var result = OpenAiOAuth2Settings.updateServiceSettingsIfPresent(null, map, validationException);

        assertNull(result);
        assertThat(validationException.validationErrors(), is(List.of(OAuth2Settings.OAUTH2_SETTINGS_NOT_CONFIGURED_ERROR)));
    }

    public void testUpdateServiceSettingsIfPresent_RetainsCurrentSettingsWhenMapEmpty() {
        var original = new OpenAiOAuth2Settings(TEST_CLIENT_ID, TEST_SCOPES, TEST_AUTH_URL);
        var validationException = new ValidationException();

        var result = OpenAiOAuth2Settings.updateServiceSettingsIfPresent(original, new HashMap<>(), validationException);

        assertThat(result, is(original));
        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testUpdateServiceSettingsIfPresent_UpdatesSettingsWhenCurrentSettingsPresent() {
        var original = new OpenAiOAuth2Settings(TEST_CLIENT_ID, TEST_SCOPES, INITIAL_TEST_AUTH_URL);
        var map = new HashMap<String, Object>();
        map.put(AUTH_URL, TEST_AUTH_URL.toString());

        var validationException = new ValidationException();

        var result = OpenAiOAuth2Settings.updateServiceSettingsIfPresent(original, map, validationException);

        assertThat(result.authUrl(), is(TEST_AUTH_URL));
        assertThat(result.clientId(), is(TEST_CLIENT_ID));
        assertThat(result.scopes(), is(TEST_SCOPES));
        assertTrue(validationException.validationErrors().isEmpty());
    }
}
