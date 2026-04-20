/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class OAuth2SettingsTests extends AbstractBWCWireSerializationTestCase<OAuth2Settings> {

    public static final String TEST_CLIENT_ID = "some_client_id";
    public static final String INITIAL_TEST_CLIENT_ID = "initial_client_id";
    public static final List<String> TEST_SCOPES = List.of("scope_1", "scope_2");
    public static final List<String> INITIAL_TEST_SCOPES = List.of("initial_scope_1", "initial_scope_2");

    public static OAuth2Settings createRandom() {
        var clientId = randomAlphaOfLength(10);
        var scopes = randomList(1, 5, () -> randomAlphaOfLength(10));
        return new OAuth2Settings(clientId, scopes);
    }

    public static Map<String, Object> addOAuth2FieldsToMap(
        Map<String, Object> map,
        @Nullable String clientId,
        @Nullable List<String> scopes
    ) {
        Objects.requireNonNull(map);

        if (clientId != null) {
            map.put(OAuth2Settings.CLIENT_ID_FIELD, clientId);
        }
        if (scopes != null) {
            map.put(OAuth2Settings.SCOPES_FIELD, scopes);
        }
        return map;
    }

    public void testFromMap_WithAllRequiredFields_CreatesSettings() {
        var map = addOAuth2FieldsToMap(new HashMap<>(), TEST_CLIENT_ID, TEST_SCOPES);

        var validationException = new ValidationException();
        var validationResult = OAuth2Settings.fromMap(map, validationException);

        assertTrue(validationResult.isSuccess());
        var settings = validationResult.result();
        assertNotNull(settings);
        assertThat(settings, is(new OAuth2Settings(TEST_CLIENT_ID, TEST_SCOPES)));
        assertThat(validationException.validationErrors(), is(empty()));
    }

    public void testFromMap_WithNoFields_ReturnsValidationResultUndefined() {
        var map = new HashMap<String, Object>();
        var validationException = new ValidationException();

        var settings = OAuth2Settings.fromMap(map, validationException);

        assertTrue(settings.isUndefined());
        assertThat(validationException.validationErrors(), is(empty()));
    }

    public void testFromMap_WithNoScopes_AddsValidationError() {
        assertFromMap_WithMissingField_AddsValidationError(
            addOAuth2FieldsToMap(new HashMap<>(), TEST_CLIENT_ID, null),
            OAuth2Settings.SCOPES_FIELD
        );
    }

    public void testFromMap_WithNoClientId_AddsValidationError() {
        assertFromMap_WithMissingField_AddsValidationError(
            addOAuth2FieldsToMap(new HashMap<>(), null, TEST_SCOPES),
            OAuth2Settings.CLIENT_ID_FIELD
        );
    }

    private static void assertFromMap_WithMissingField_AddsValidationError(Map<String, Object> serviceSettingsMap, String missingField) {
        var validationException = new ValidationException();

        var settings = OAuth2Settings.fromMap(serviceSettingsMap, validationException);

        assertTrue(settings.isFailed());

        var expectedError = Strings.format(
            "[%s] OAuth2 fields [%s] must be provided together; missing: [%s]",
            ModelConfigurations.SERVICE_SETTINGS,
            OAuth2Settings.REQUIRED_FIELDS,
            missingField
        );
        var errors = validationException.validationErrors();
        assertThat(errors, hasSize(1));
        assertThat(errors, contains(expectedError));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var settings = new OAuth2Settings(TEST_CLIENT_ID, TEST_SCOPES);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        settings.toXContent(builder, null);
        builder.endObject();
        var xContentResult = Strings.toString(builder);

        assertThat(XContentHelper.stripWhitespace(xContentResult), is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "client_id": "%s",
                "scopes": %s
            }
            """, TEST_CLIENT_ID, toJson(TEST_SCOPES, "")))));
    }

    public void testUpdateServiceSettings_AllFields_UpdatesSettings() {
        assertUpdateServiceSettings(addOAuth2FieldsToMap(new HashMap<>(), TEST_CLIENT_ID, TEST_SCOPES), TEST_CLIENT_ID, TEST_SCOPES);
    }

    public void testUpdateServiceSettings_WithMissingClientId_UpdatesSettings() {
        assertUpdateServiceSettings(addOAuth2FieldsToMap(new HashMap<>(), null, TEST_SCOPES), INITIAL_TEST_CLIENT_ID, TEST_SCOPES);
    }

    public void testUpdateServiceSettings_WithMissingScopes_UpdatesSettings() {
        assertUpdateServiceSettings(addOAuth2FieldsToMap(new HashMap<>(), TEST_CLIENT_ID, null), TEST_CLIENT_ID, INITIAL_TEST_SCOPES);
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        assertUpdateServiceSettings(new HashMap<>(), INITIAL_TEST_CLIENT_ID, INITIAL_TEST_SCOPES);
    }

    private static void assertUpdateServiceSettings(
        Map<String, Object> serviceSettingsMap,
        @Nullable String clientId,
        @Nullable List<String> scopes
    ) {
        var originalSettings = new OAuth2Settings(INITIAL_TEST_CLIENT_ID, INITIAL_TEST_SCOPES);
        var validationException = new ValidationException();

        var updatedSettings = originalSettings.updateServiceSettings(serviceSettingsMap, validationException);

        assertThat(updatedSettings, is(new OAuth2Settings(clientId, scopes)));
        assertThat(validationException.validationErrors(), is(empty()));
    }

    @Override
    protected Writeable.Reader<OAuth2Settings> instanceReader() {
        return OAuth2Settings::new;
    }

    @Override
    protected OAuth2Settings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OAuth2Settings mutateInstance(OAuth2Settings instance) throws IOException {
        var clientId = instance.clientId();
        var scopes = instance.scopes();
        switch (randomInt(1)) {
            case 0 -> clientId = randomValueOtherThan(clientId, () -> randomAlphaOfLength(12));
            case 1 -> scopes = randomValueOtherThan(scopes, () -> randomList(1, 5, () -> randomAlphaOfLength(10)));
            default -> throw new AssertionError("Illegal randomization branch");
        }
        return new OAuth2Settings(clientId, scopes);
    }

    @Override
    protected OAuth2Settings mutateInstanceForVersion(OAuth2Settings instance, TransportVersion version) {
        return instance;
    }
}
