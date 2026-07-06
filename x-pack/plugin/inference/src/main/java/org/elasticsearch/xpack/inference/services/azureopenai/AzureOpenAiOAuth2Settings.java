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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.oauth2.BaseOAuth2Settings;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings.getOAuth2Configurations;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

/**
 * Represents the OAuth2 service-level settings required for Azure OpenAI client credentials flow authentication,
 * which includes the standard OAuth2 fields (client ID and scopes)  as well as the tenant ID specific to Azure.
 */
public class AzureOpenAiOAuth2Settings extends BaseOAuth2Settings {

    public static final TransportVersion AZURE_OPENAI_OAUTH_SETTINGS = TransportVersion.fromName("azure_openai_oauth_settings");

    public static final String TENANT_ID_FIELD = "tenant_id";

    public static final Set<String> REQUIRED_FIELDS = Sets.addToCopy(OAuth2Settings.REQUIRED_FIELDS, TENANT_ID_FIELD);
    public static final String REQUIRED_FIELDS_DESCRIPTION = requiredFieldsDescription(REQUIRED_FIELDS);

    private static final String TENANT_ID_CONFIG_DESCRIPTION = "The directory tenant that you want to request permission from.";
    private static final String SERVICE_DESCRIPTION = "Azure OpenAI";

    private final String tenantId;

    public static AzureOpenAiOAuth2Settings fromMap(Map<String, Object> map, ValidationException validationException) {
        var oauth2ServiceSettings = OAuth2Settings.fromMap(map, validationException);
        var tenantId = extractOptionalString(map, TENANT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);

        var hasAllFields = validateFields(oauth2ServiceSettings, tenantId, TENANT_ID_FIELD, SERVICE_DESCRIPTION, validationException);

        if (hasAllFields) {
            return new AzureOpenAiOAuth2Settings(oauth2ServiceSettings.result(), tenantId);
        }

        return null;
    }

    public static boolean hasAnyOAuth2Fields(Map<String, Object> map) {
        return OAuth2Settings.hasAnyOAuth2Fields(map) || map.containsKey(TENANT_ID_FIELD);
    }

    public AzureOpenAiOAuth2Settings(OAuth2Settings oAuth2Settings, String tenantId) {
        super(oAuth2Settings);
        this.tenantId = Objects.requireNonNull(tenantId);
    }

    public AzureOpenAiOAuth2Settings(StreamInput in) throws IOException {
        this(new OAuth2Settings(in), in.readString());
    }

    public String tenantId() {
        return tenantId;
    }

    /**
     * Updates the current settings with any new values provided in the map.
     * If a field is not present in the map, the existing value is retained.
     * @param serviceSettingsMap the map containing the new settings values
     * @param validationException the validation exception to which any validation errors will be added
     * @return a new {@link AzureOpenAiOAuth2Settings} object with the updated values from the map, or existing values if not updated
     */
    public AzureOpenAiOAuth2Settings updateServiceSettings(
        Map<String, Object> serviceSettingsMap,
        ValidationException validationException
    ) {
        var updatedOauth2ServiceSettings = oAuth2Settings.updateServiceSettings(serviceSettingsMap, validationException);
        var extractedTenantId = extractOptionalString(
            serviceSettingsMap,
            TENANT_ID_FIELD,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        return new AzureOpenAiOAuth2Settings(updatedOauth2ServiceSettings, extractedTenantId != null ? extractedTenantId : this.tenantId);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        oAuth2Settings.writeTo(out);
        out.writeString(tenantId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        oAuth2Settings.toXContent(builder, params);
        builder.field(TENANT_ID_FIELD, tenantId);
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureOpenAiOAuth2Settings that = (AzureOpenAiOAuth2Settings) o;
        return Objects.equals(oAuth2Settings, that.oAuth2Settings) && Objects.equals(tenantId, that.tenantId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oAuth2Settings, tenantId);
    }

    public static Map<String, SettingsConfiguration> configurations(EnumSet<TaskType> supportedTaskTypes) {
        var config = new HashMap<>(
            Map.of(
                TENANT_ID_FIELD,
                new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(TENANT_ID_CONFIG_DESCRIPTION)
                    .setLabel("OAuth2 Tenant ID")
                    .setRequired(false)
                    .setSensitive(false)
                    .setUpdatable(true)
                    .setType(SettingsConfigurationFieldType.STRING)
                    .build()
            )
        );

        config.putAll(getOAuth2Configurations(supportedTaskTypes));
        return config;
    }
}
