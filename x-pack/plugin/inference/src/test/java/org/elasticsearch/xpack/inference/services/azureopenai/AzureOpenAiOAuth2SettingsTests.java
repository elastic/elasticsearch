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
import org.elasticsearch.inference.ModelConfigurations;
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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AzureOpenAiOAuth2SettingsTests extends AbstractBWCWireSerializationTestCase<AzureOpenAiOAuth2Settings> {

    public static final String TEST_TENANT_ID = "some_tenant_id";
    public static final String INITIAL_TEST_TENANT_ID = "initial_tenant_id";

    public static AzureOpenAiOAuth2Settings createRandom() {
        var coreOAuth2Settings = OAuth2SettingsTests.createRandom();
        var tenantId = randomAlphaOfLength(10);
        return new AzureOpenAiOAuth2Settings(coreOAuth2Settings, tenantId);
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
        var settingsMap = addAzureOpenAiOAuth2FieldsToMap(
            new HashMap<>(),
            OAuth2SettingsTests.TEST_CLIENT_ID,
            OAuth2SettingsTests.TEST_SCOPES,
            TEST_TENANT_ID
        );
        var validationException = new ValidationException();

        var settings = AzureOpenAiOAuth2Settings.fromMap(settingsMap, validationException);

        assertThat(
            settings,
            is(
                new AzureOpenAiOAuth2Settings(
                    new OAuth2Settings(OAuth2SettingsTests.TEST_CLIENT_ID, OAuth2SettingsTests.TEST_SCOPES),
                    TEST_TENANT_ID
                )
            )
        );
        assertThat(validationException.validationErrors(), is(empty()));
    }

    public void testFromMap_WithNoOAuthFields_ReturnsNull() {
        var validationException = new ValidationException();

        var settings = AzureOpenAiOAuth2Settings.fromMap(new HashMap<>(), validationException);

        assertThat(settings, is(nullValue()));
        assertThat(validationException.validationErrors(), is(empty()));
    }

    public void testFromMap_WithOnlyTenantId_AddsValidationErrorForClientIdAndScopes() {
        var settingsMap = addAzureOpenAiOAuth2FieldsToMap(new HashMap<>(), null, null, TEST_TENANT_ID);
        var validationException = new ValidationException();

        var settings = AzureOpenAiOAuth2Settings.fromMap(settingsMap, validationException);

        assertThat(settings, is(nullValue()));

        var expectedError = Strings.format(
            "[%s] all Azure OpenAI OAuth2 fields must be provided together; missing: [%s]",
            ModelConfigurations.SERVICE_SETTINGS,
            OAuth2Settings.REQUIRED_FIELDS
        );
        var errors = validationException.validationErrors();
        assertThat(errors, hasSize(1));
        assertThat(errors, contains(expectedError));
    }

    public void testFromMap_WithOnlyClientIdAndScopes_AddsValidationErrorForTenantId() {
        var settingsMap = addAzureOpenAiOAuth2FieldsToMap(
            new HashMap<>(),
            OAuth2SettingsTests.TEST_CLIENT_ID,
            OAuth2SettingsTests.TEST_SCOPES,
            null
        );
        var validationException = new ValidationException();

        var settings = AzureOpenAiOAuth2Settings.fromMap(settingsMap, validationException);

        assertThat(settings, is(nullValue()));

        var expectedError = Strings.format(
            "[%s] all Azure OpenAI OAuth2 fields must be provided together; missing: [%s]",
            ModelConfigurations.SERVICE_SETTINGS,
            AzureOpenAiOAuth2Settings.TENANT_ID_FIELD
        );
        var errors = validationException.validationErrors();
        assertThat(errors, hasSize(1));
        assertThat(errors, contains(expectedError));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var settings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.TEST_CLIENT_ID, OAuth2SettingsTests.TEST_SCOPES),
            TEST_TENANT_ID
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        settings.toXContent(builder, null);
        builder.endObject();
        var xContentResult = Strings.toString(builder);

        assertThat(XContentHelper.stripWhitespace(xContentResult), is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "client_id": "%s",
                "scopes": %s,
                "tenant_id": "%s"
            }
            """, OAuth2SettingsTests.TEST_CLIENT_ID, JsonUtils.toJson(OAuth2SettingsTests.TEST_SCOPES, ""), TEST_TENANT_ID))));
    }

    public void testUpdateServiceSettings_AllFields_FieldsAreUpdated() {
        assertUpdateServiceSettings(
            addAzureOpenAiOAuth2FieldsToMap(
                new HashMap<>(),
                OAuth2SettingsTests.TEST_CLIENT_ID,
                OAuth2SettingsTests.TEST_SCOPES,
                TEST_TENANT_ID
            ),
            new AzureOpenAiOAuth2Settings(
                new OAuth2Settings(OAuth2SettingsTests.TEST_CLIENT_ID, OAuth2SettingsTests.TEST_SCOPES),
                TEST_TENANT_ID
            )
        );
    }

    public void testUpdateServiceSettings_WithMissingClientId_FieldsAreUpdated() {
        assertUpdateServiceSettings(
            addAzureOpenAiOAuth2FieldsToMap(new HashMap<>(), null, OAuth2SettingsTests.TEST_SCOPES, TEST_TENANT_ID),
            new AzureOpenAiOAuth2Settings(
                new OAuth2Settings(OAuth2SettingsTests.INITIAL_TEST_CLIENT_ID, OAuth2SettingsTests.TEST_SCOPES),
                TEST_TENANT_ID
            )
        );
    }

    public void testUpdateServiceSettings_WithMissingScopes_FieldsAreUpdated() {
        assertUpdateServiceSettings(
            addAzureOpenAiOAuth2FieldsToMap(new HashMap<>(), OAuth2SettingsTests.TEST_CLIENT_ID, null, TEST_TENANT_ID),
            new AzureOpenAiOAuth2Settings(
                new OAuth2Settings(OAuth2SettingsTests.TEST_CLIENT_ID, OAuth2SettingsTests.INITIAL_TEST_SCOPES),
                TEST_TENANT_ID
            )
        );
    }

    public void testUpdateServiceSettings_WithMissingTenantId_FieldsAreUpdated() {
        assertUpdateServiceSettings(
            addAzureOpenAiOAuth2FieldsToMap(new HashMap<>(), OAuth2SettingsTests.TEST_CLIENT_ID, OAuth2SettingsTests.TEST_SCOPES, null),
            new AzureOpenAiOAuth2Settings(
                new OAuth2Settings(OAuth2SettingsTests.TEST_CLIENT_ID, OAuth2SettingsTests.TEST_SCOPES),
                INITIAL_TEST_TENANT_ID
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        assertUpdateServiceSettings(
            new HashMap<>(),
            new AzureOpenAiOAuth2Settings(
                new OAuth2Settings(OAuth2SettingsTests.INITIAL_TEST_CLIENT_ID, OAuth2SettingsTests.INITIAL_TEST_SCOPES),
                INITIAL_TEST_TENANT_ID
            )
        );
    }

    private static void assertUpdateServiceSettings(Map<String, Object> serviceSettingsMap, AzureOpenAiOAuth2Settings expectedSettings) {
        var originalSettings = new AzureOpenAiOAuth2Settings(
            new OAuth2Settings(OAuth2SettingsTests.INITIAL_TEST_CLIENT_ID, OAuth2SettingsTests.INITIAL_TEST_SCOPES),
            INITIAL_TEST_TENANT_ID
        );
        var validationException = new ValidationException();

        var updatedSettings = originalSettings.updateServiceSettings(serviceSettingsMap, validationException);

        assertThat(updatedSettings, is(expectedSettings));
        assertThat(validationException.validationErrors(), is(empty()));
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
            case 1 -> scopes = randomValueOtherThan(scopes, () -> randomList(1, 5, () -> randomAlphaOfLength(10)));
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
