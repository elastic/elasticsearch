/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class OpenAiOAuth2SettingsTests extends AbstractBWCWireSerializationTestCase<OpenAiOAuth2Settings> {

    public static final URI TEST_AUTH_URL = URI.create("https://idp.example.com/oauth2/token");
    public static final URI INITIAL_TEST_AUTH_URL = URI.create("https://initial-idp.example.com/oauth2/token");

    private static final String CLIENT_ID = "client-id";
    private static final List<String> SCOPES = List.of("scope-a", "scope-b");

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
        map.put("client_id", CLIENT_ID);
        map.put("scopes", SCOPES);
        map.put("auth_url", TEST_AUTH_URL.toString());

        var validationException = new ValidationException();
        var result = OpenAiOAuth2Settings.fromMap(map, validationException);

        assertNotNull(result);
        assertEquals(CLIENT_ID, result.clientId());
        assertEquals(SCOPES, result.scopes());
        assertEquals(TEST_AUTH_URL, result.authUrl());
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
        map.put("client_id", CLIENT_ID);
        map.put("scopes", SCOPES);

        var validationException = new ValidationException();
        var result = OpenAiOAuth2Settings.fromMap(map, validationException);

        assertNull(result);
        assertFalse(validationException.validationErrors().isEmpty());
        assertThat(validationException.validationErrors().get(0), containsString("auth_url"));
    }

    public void testFromMap_ValidationErrorWhenClientIdMissingButAuthUrlPresent() {
        var map = new HashMap<String, Object>();
        map.put("auth_url", TEST_AUTH_URL.toString());

        var validationException = new ValidationException();
        var result = OpenAiOAuth2Settings.fromMap(map, validationException);

        assertNull(result);
        assertFalse(validationException.validationErrors().isEmpty());
    }

    public void testHasAnyOAuth2Fields_ReturnsTrueForAuthUrlOnly() {
        Map<String, Object> map = Map.of("auth_url", TEST_AUTH_URL.toString());
        assertTrue(OpenAiOAuth2Settings.hasAnyOAuth2Fields(map));
    }

    public void testUpdateServiceSettings_RetainsExistingFieldsWhenMapEmpty() {
        var original = new OpenAiOAuth2Settings(CLIENT_ID, SCOPES, TEST_AUTH_URL);
        var validationException = new ValidationException();

        var updated = original.updateServiceSettings(new HashMap<>(), validationException);

        assertEquals(original, updated);
        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testUpdateServiceSettings_OverridesAuthUrl() {
        var original = new OpenAiOAuth2Settings(CLIENT_ID, SCOPES, INITIAL_TEST_AUTH_URL);

        var map = new HashMap<String, Object>();
        map.put("auth_url", TEST_AUTH_URL.toString());

        var updated = original.updateServiceSettings(map, new ValidationException());

        assertEquals(TEST_AUTH_URL, updated.authUrl());
        assertEquals(original.clientId(), updated.clientId());
        assertEquals(original.scopes(), updated.scopes());
    }
}
