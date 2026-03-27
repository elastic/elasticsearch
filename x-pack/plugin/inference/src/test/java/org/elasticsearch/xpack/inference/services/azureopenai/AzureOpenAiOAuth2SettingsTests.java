/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

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
import org.elasticsearch.xpack.inference.common.JsonUtils;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2SettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class AzureOpenAiOAuth2SettingsTests extends AbstractBWCWireSerializationTestCase<AzureOpenAiOAuth2Settings> {

    private static final String TENANT_ID_VALUE = "some_tenant_id";

    public static AzureOpenAiOAuth2Settings createRandom() {
        return new AzureOpenAiOAuth2Settings(OAuth2SettingsTests.createRandom(), randomAlphaOfLength(10));
    }

    public static Map<String, Object> addAzureOpenAiOAuth2FieldsToMap(
        Map<String, Object> map,
        @Nullable String clientId,
        @Nullable List<String> scopes,
        @Nullable String tenantId
    ) {
        OAuth2SettingsTests.addOAuth2FieldsToMap(map, clientId, scopes);
        if (tenantId != null) {
            map.put(AzureOpenAiOAuth2Settings.TENANT_ID_FIELD, tenantId);
        }
        return map;
    }

    public void testFromMap_WithAllRequiredFields_CreatesSettings() {
        var map = new HashMap<String, Object>();
        OAuth2SettingsTests.addOAuth2FieldsToMap(map, OAuth2SettingsTests.CLIENT_ID_VALUE, OAuth2SettingsTests.SCOPES_VALUE);
        map.put(AzureOpenAiOAuth2Settings.TENANT_ID_FIELD, TENANT_ID_VALUE);

        var validationException = new ValidationException();
        var settings = AzureOpenAiOAuth2Settings.fromMap(map, validationException);

        assertNotNull(settings);
        assertThat(settings.clientId(), is(OAuth2SettingsTests.CLIENT_ID_VALUE));
        assertThat(settings.scopes(), is(OAuth2SettingsTests.SCOPES_VALUE));
        assertThat(settings.tenantId(), is(TENANT_ID_VALUE));
        assertThat(validationException.validationErrors(), is(empty()));
    }

    public void testFromMap_WithNoOAuthFields_ReturnsNull() {
        var map = new HashMap<String, Object>();
        var validationException = new ValidationException();

        var settings = AzureOpenAiOAuth2Settings.fromMap(map, validationException);

        assertNull(settings);
        assertTrue(validationException.validationErrors().isEmpty());
    }

    public void testFromMap_WithOnlyTenantId_AddsValidationErrorForClientIdAndScopes() {
        var map = new HashMap<String, Object>();
        map.put(AzureOpenAiOAuth2Settings.TENANT_ID_FIELD, TENANT_ID_VALUE);
        var validationException = new ValidationException();

        var settings = AzureOpenAiOAuth2Settings.fromMap(map, validationException);

        assertNull(settings);

        var thrownException = expectThrows(ValidationException.class, validationException::throwIfValidationErrorsExist);

        assertThat(thrownException.getMessage(), containsString("all Azure OpenAI OAuth2 fields must be provided together"));
        assertThat(thrownException.getMessage(), containsString(OAuth2Settings.REQUIRED_FIELDS));
        assertThat(thrownException.getMessage(), not(containsString(AzureOpenAiOAuth2Settings.TENANT_ID_FIELD)));
    }

    public void testFromMap_WithOnlyClientIdAndScopes_AddsValidationError() {
        var map = new HashMap<String, Object>();
        OAuth2SettingsTests.addOAuth2FieldsToMap(map, OAuth2SettingsTests.CLIENT_ID_VALUE, OAuth2SettingsTests.SCOPES_VALUE);
        var validationException = new ValidationException();

        AzureOpenAiOAuth2Settings.fromMap(map, validationException);
        var thrownException = expectThrows(ValidationException.class, validationException::throwIfValidationErrorsExist);

        assertThat(thrownException.getMessage(), containsString("all Azure OpenAI OAuth2 fields must be provided together"));
        assertThat(thrownException.getMessage(), containsString(AzureOpenAiOAuth2Settings.TENANT_ID_FIELD));
        assertThat(thrownException.getMessage(), not(containsString(OAuth2Settings.CLIENT_ID_FIELD)));
        assertThat(thrownException.getMessage(), not(containsString(OAuth2Settings.SCOPES_FIELD)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID_VALUE, OAuth2SettingsTests.SCOPES_VALUE),
            TENANT_ID_VALUE
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        settings.toXContent(builder, null);
        builder.endObject();
        var xContentResult = Strings.toString(builder);

        assertThat(XContentHelper.stripWhitespace(xContentResult), is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "client_id":"%s",
                "scopes":%s,
                "tenant_id":"%s"
            }
            """, OAuth2SettingsTests.CLIENT_ID_VALUE, JsonUtils.toJson(OAuth2SettingsTests.SCOPES_VALUE, ""), TENANT_ID_VALUE))));
    }

    public void testUpdateServiceSettings_EmptyUpdateMap_KeepsOriginalValues() {
        var settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID_VALUE, OAuth2SettingsTests.SCOPES_VALUE),
            TENANT_ID_VALUE
        );
        var validationException = new ValidationException();

        var updated = settings.updateServiceSettings(new HashMap<>(), validationException);

        assertThat(updated, is(settings));
    }

    public void testUpdateServiceSettings_UpdateTenantId_ReplacesTenantId() {
        var settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID_VALUE, OAuth2SettingsTests.SCOPES_VALUE),
            TENANT_ID_VALUE
        );
        var newTenantId = "new-tenant";
        var validationException = new ValidationException();

        var updated = settings.updateServiceSettings(
            new HashMap<>(Map.of(AzureOpenAiOAuth2Settings.TENANT_ID_FIELD, newTenantId)),
            validationException
        );

        assertThat(updated.clientId(), is(OAuth2SettingsTests.CLIENT_ID_VALUE));
        assertThat(updated.scopes(), is(OAuth2SettingsTests.SCOPES_VALUE));
        assertThat(updated.tenantId(), is(newTenantId));
    }

    public void testUpdateServiceSettings_UpdateClientIdAndScopes_ReplacesOAuthFields() {
        var settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.CLIENT_ID_VALUE, OAuth2SettingsTests.SCOPES_VALUE),
            TENANT_ID_VALUE
        );
        var newClientId = "new-client";
        var newScopes = List.of("new-scope1", "new-scope2");
        var validationException = new ValidationException();

        var updated = settings.updateServiceSettings(
            new HashMap<>(Map.of(OAuth2Settings.CLIENT_ID_FIELD, newClientId, OAuth2Settings.SCOPES_FIELD, newScopes)),
            validationException
        );

        assertThat(updated.clientId(), is(newClientId));
        assertThat(updated.scopes(), is(newScopes));
        assertThat(updated.tenantId(), is(TENANT_ID_VALUE));
    }

    @Override
    protected Writeable.Reader<AzureOpenAiOAuth2Settings> instanceReader() {
        return AzureOpenAiOAuth2Settings::new;
    }

    @Override
    protected AzureOpenAiOAuth2Settings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureOpenAiOAuth2Settings mutateInstance(AzureOpenAiOAuth2Settings instance) throws IOException {
        var clientId = instance.clientId();
        var scopes = instance.scopes();
        var tenantId = instance.tenantId();
        switch (randomInt(2)) {
            case 0 -> clientId = randomValueOtherThan(clientId, () -> randomAlphaOfLength(12));
            case 1 -> scopes = randomValueOtherThan(scopes, () -> List.of(randomAlphaOfLength(10)));
            case 2 -> tenantId = randomValueOtherThan(tenantId, () -> randomAlphaOfLength(8));
            default -> throw new AssertionError("Illegal randomization branch");
        }
        return new AzureOpenAiOAuth2Settings(new OAuth2Settings(clientId, scopes), tenantId);
    }

    @Override
    protected AzureOpenAiOAuth2Settings mutateInstanceForVersion(AzureOpenAiOAuth2Settings instance, TransportVersion version) {
        return instance;
    }
}
