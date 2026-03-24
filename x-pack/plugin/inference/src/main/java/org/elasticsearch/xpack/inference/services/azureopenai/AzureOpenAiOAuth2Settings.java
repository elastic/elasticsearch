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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.ValidationResult;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings.getOAuth2Configurations;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

/**
 * Represents the OAuth2 service-level settings required for Azure OpenAI client credentials flow authentication,
 * which includes the standard OAuth2 fields (client ID and scopes)  as well as the tenant ID specific to Azure.
 */
public class AzureOpenAiOAuth2Settings implements ToXContentFragment, Writeable {

    public static final TransportVersion AZURE_OPENAI_OAUTH_SETTINGS = TransportVersion.fromName("azure_openai_oauth_settings");

    public static final String TENANT_ID_FIELD = "tenant_id";

    public static final String REQUIRED_FIELDS = String.join(", ", OAuth2Settings.REQUIRED_FIELDS, TENANT_ID_FIELD);

    public static final String REQUIRED_FIELDS_DESCRIPTION = Strings.format("OAuth2 requires the fields [%s], to be set.", REQUIRED_FIELDS);

    private static final String TENANT_ID_CONFIG_DESCRIPTION = "The directory tenant that you want to request permission from.";

    private record UpdateSettings(ValidationResult<OAuth2Settings> oauth2Settings, @Nullable String tenantId) {
        UpdateSettings {
            Objects.requireNonNull(oauth2Settings);
        }
    }

    private final OAuth2Settings oauth2Settings;
    private final String tenantId;

    public static AzureOpenAiOAuth2Settings fromMap(Map<String, Object> map, ValidationException validationException) {
        var oauth2ServiceSettings = OAuth2Settings.fromMap(map, validationException);
        var tenantId = extractOptionalString(map, TENANT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);

        var hasAllFields = validateFields(oauth2ServiceSettings, tenantId, validationException);

        if (hasAllFields) {
            return new AzureOpenAiOAuth2Settings(oauth2ServiceSettings.result(), tenantId);
        }

        return null;
    }

    public static boolean hasAnyOAuth2Fields(Map<String, Object> map) {
        return OAuth2Settings.hasAnyOAuth2Fields(map) || map.containsKey(TENANT_ID_FIELD);
    }

    /**
     * Validates that either all or none of the fields are provided. If any field is provided, then all fields must be provided.
     * This is because OAuth2 requires all fields to be set together.
     *
     * @return true if all fields are provided, false if no fields are provided or some are missing
     */
    private static boolean validateFields(
        ValidationResult<OAuth2Settings> oauth2Settings,
        @Nullable String tenantId,
        ValidationException validationException
    ) {
        if (tenantId == null) {
            if (oauth2Settings.isUndefined()) {
                return false;
            }

            addMissingFieldsValidationException(TENANT_ID_FIELD, validationException);

            return false;
        } else if (oauth2Settings.isUndefined()) {
            addMissingFieldsValidationException(OAuth2Settings.REQUIRED_FIELDS, validationException);
            return false;
        }

        return oauth2Settings.isSuccess();
    }

    private static void addMissingFieldsValidationException(String missingFields, ValidationException validationException) {
        validationException.addValidationError(
            Strings.format(
                "[%s] all Azure OpenAI OAuth2 fields must be provided together; missing: [%s]",
                ModelConfigurations.SERVICE_SETTINGS,
                missingFields
            )
        );
    }

    public AzureOpenAiOAuth2Settings(OAuth2Settings oauth2Settings, String tenantId) {
        this.oauth2Settings = Objects.requireNonNull(oauth2Settings);
        this.tenantId = Objects.requireNonNull(tenantId);
    }

    public AzureOpenAiOAuth2Settings(StreamInput in) throws IOException {
        this.oauth2Settings = new OAuth2Settings(in);
        this.tenantId = in.readString();
    }

    public String clientId() {
        return oauth2Settings.clientId();
    }

    public String tenantId() {
        return tenantId;
    }

    public List<String> scopes() {
        return oauth2Settings.scopes();
    }

    public AzureOpenAiOAuth2Settings updateServiceSettings(Map<String, Object> serviceSettings, ValidationException validationException) {
        var updated = fromMapForUpdate(serviceSettings, oauth2Settings, validationException);

        var tenantIdToUpdate = updated.tenantId() != null ? updated.tenantId() : this.tenantId;

        var hasAllFields = validateFields(updated.oauth2Settings(), tenantIdToUpdate, validationException);

        if (hasAllFields == false) {
            return this;
        }

        return new AzureOpenAiOAuth2Settings(updated.oauth2Settings().result(), tenantIdToUpdate);
    }

    private static UpdateSettings fromMapForUpdate(
        Map<String, Object> map,
        OAuth2Settings oAuth2Settings,
        ValidationException validationException
    ) {
        var oauth2ServiceSettings = oAuth2Settings.updateServiceSettings(map, validationException);
        var tenantId = extractOptionalString(map, TENANT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);

        return new UpdateSettings(oauth2ServiceSettings, tenantId);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        oauth2Settings.writeTo(out);
        out.writeString(tenantId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        oauth2Settings.toXContent(builder, params);
        builder.field(TENANT_ID_FIELD, tenantId);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureOpenAiOAuth2Settings that = (AzureOpenAiOAuth2Settings) o;
        return Objects.equals(oauth2Settings, that.oauth2Settings) && Objects.equals(tenantId, that.tenantId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oauth2Settings, tenantId);
    }

    public static Map<String, SettingsConfiguration> configurations(EnumSet<TaskType> supportedTaskTypes) {
        var config = new HashMap<>(
            Map.of(
                TENANT_ID_FIELD,
                new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(TENANT_ID_CONFIG_DESCRIPTION)
                    .setLabel("OAuth2 Tenant ID")
                    .setRequired(false)
                    .setSensitive(true)
                    .setUpdatable(true)
                    .setType(SettingsConfigurationFieldType.STRING)
                    .build()
            )
        );

        config.putAll(getOAuth2Configurations(supportedTaskTypes));
        return config;
    }
}
