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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class OAuth2SettingsTests extends AbstractBWCWireSerializationTestCase<OAuth2Settings> {

    public static final String CLIENT_ID_VALUE = "some_client_id";
    public static final List<String> SCOPES_VALUE = List.of("scope1", "scope2");

    public static OAuth2Settings createRandom() {
        return new OAuth2Settings(randomAlphaOfLength(10), randomList(5, () -> randomAlphaOfLength(10)));
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
        var map = addOAuth2FieldsToMap(new HashMap<>(), CLIENT_ID_VALUE, SCOPES_VALUE);

        var validationException = new ValidationException();
        var validationResult = OAuth2Settings.fromMap(map, validationException);

        assertTrue(validationResult.isSuccess());
        var settings = validationResult.result();
        assertNotNull(settings);
        assertThat(settings.clientId(), is(CLIENT_ID_VALUE));
        assertThat(settings.scopes(), is(SCOPES_VALUE));
        assertThat(validationException.validationErrors(), is(empty()));
    }

    public void testFromMap_WithNoFields_ReturnsValidationResultUndefined() {
        var map = new HashMap<String, Object>();
        var validationException = new ValidationException();

        var settings = OAuth2Settings.fromMap(map, validationException);

        assertTrue(settings.isUndefined());
        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testFromMap_WithOnlyClientId_AddsValidationError() {
        var map = new HashMap<String, Object>();
        map.put(OAuth2Settings.CLIENT_ID_FIELD, CLIENT_ID_VALUE);
        var validationException = new ValidationException();

        OAuth2Settings.fromMap(map, validationException);
        var thrownException = expectThrows(ValidationException.class, validationException::throwIfValidationErrorsExist);

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "OAuth2 fields [%s] must be provided together; missing: [%s]",
                    OAuth2Settings.REQUIRED_FIELDS,
                    OAuth2Settings.SCOPES_FIELD
                )
            )
        );
    }

    public void testFromMap_WithOnlyScopes_AddsValidationError() {
        var map = new HashMap<String, Object>();
        map.put(OAuth2Settings.SCOPES_FIELD, SCOPES_VALUE);
        var validationException = new ValidationException();

        OAuth2Settings.fromMap(map, validationException);
        var thrownException = expectThrows(ValidationException.class, validationException::throwIfValidationErrorsExist);

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "OAuth2 fields [%s] must be provided together; missing: [%s]",
                    OAuth2Settings.REQUIRED_FIELDS,
                    OAuth2Settings.CLIENT_ID_FIELD
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var settings = new OAuth2Settings(CLIENT_ID_VALUE, SCOPES_VALUE);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        settings.toXContent(builder, null);
        builder.endObject();
        var xContentResult = Strings.toString(builder);

        assertThat(XContentHelper.stripWhitespace(xContentResult), is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "client_id":"%s",
                "scopes":%s
            }
            """, CLIENT_ID_VALUE, toJson(SCOPES_VALUE, "")))));
    }

    public void testUpdateServiceSettings_EmptyUpdateMap_KeepsOriginalValues() {
        var settings = new OAuth2Settings(CLIENT_ID_VALUE, SCOPES_VALUE);
        var validationException = new ValidationException();

        var validationResult = settings.updateServiceSettings(new HashMap<>(), validationException);
        assertTrue(validationResult.isSuccess());

        var updated = validationResult.result();
        assertThat(updated, is(settings));
    }

    public void testUpdateServiceSettings_UpdateClientId_ReplacesClientId() {
        var settings = new OAuth2Settings(CLIENT_ID_VALUE, SCOPES_VALUE);
        var validationException = new ValidationException();

        var newClientId = "new-client";
        var validationResult = settings.updateServiceSettings(
            new HashMap<>(Map.of(OAuth2Settings.CLIENT_ID_FIELD, newClientId)),
            validationException
        );

        assertTrue(validationResult.isSuccess());

        var updated = validationResult.result();
        assertNotNull(updated);
        assertThat(updated.clientId(), is(newClientId));
        assertThat(updated.scopes(), is(SCOPES_VALUE));
    }

    public void testUpdateServiceSettings_UpdateScopes_ReplacesScopes() {
        var settings = new OAuth2Settings(CLIENT_ID_VALUE, SCOPES_VALUE);
        var validationException = new ValidationException();

        var newScopes = List.of("new-scope1", "new-scope2");
        var validationResult = settings.updateServiceSettings(
            new HashMap<>(Map.of(OAuth2Settings.SCOPES_FIELD, newScopes)),
            validationException
        );

        assertTrue(validationResult.isSuccess());

        var updated = validationResult.result();
        assertNotNull(updated);
        assertThat(updated.clientId(), is(CLIENT_ID_VALUE));
        assertThat(updated.scopes(), is(newScopes));
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
            case 1 -> scopes = randomValueOtherThan(scopes, () -> List.of(randomAlphaOfLength(10)));
            default -> throw new AssertionError("Illegal randomization branch");
        }
        return new OAuth2Settings(clientId, scopes);
    }

    @Override
    protected OAuth2Settings mutateInstanceForVersion(OAuth2Settings instance, TransportVersion version) {
        return instance;
    }
}
