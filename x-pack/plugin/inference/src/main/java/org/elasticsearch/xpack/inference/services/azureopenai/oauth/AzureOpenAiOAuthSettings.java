/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.oauth;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StringParser.extractStringList;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.EXACTLY_ONE_CONFIG_DESCRIPTION;

public class AzureOpenAiOAuthSettings implements ToXContentFragment, Writeable {

    public static final TransportVersion AZURE_OPENAI_OAUTH_SETTINGS = TransportVersion.fromName("azure_openai_oauth_settings");

    public static final String CLIENT_ID_FIELD = "client_id";
    public static final String TENANT_ID_FIELD = "tenant_id";
    public static final String SCOPES_FIELD = "scopes";

    public static final String REQUIRED_FIELDS = String.join(", ", CLIENT_ID_FIELD, TENANT_ID_FIELD, SCOPES_FIELD);

    public static final String REQUIRED_FIELDS_DESCRIPTION = Strings.format("OAuth2 requires the fields [%s], to be set.", REQUIRED_FIELDS);

    private record UpdateSettings(@Nullable String clientId, @Nullable String tenantId, @Nullable List<String> scopes) {}

    private final String clientId;
    private final String tenantId;
    private final List<String> scopes;

    public static AzureOpenAiOAuthSettings fromMap(Map<String, Object> map, ValidationException validationException) {
        var clientId = extractOptionalString(map, CLIENT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var tenantId = extractOptionalString(map, TENANT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var scopes = extractStringList(map, SCOPES_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);

        var hasFields = validateFields(clientId, tenantId, scopes, validationException);

        if (hasFields) {
            return new AzureOpenAiOAuthSettings(clientId, tenantId, scopes);

        }

        return null;
    }

    /**
     * Validates that either all or none of the fields are provided. If any field is provided, then all fields must be provided.
     * This is because OAuth2 requires all fields to be set together.
     *
     * @return true if all fields are provided or false it no fields are provided. If some but not all fields are provided,
     * a ValidationException is thrown.
     */
    private static boolean validateFields(
        @Nullable String clientId,
        @Nullable String tenantId,
        @Nullable List<String> scopes,
        ValidationException validationException
    ) {
        boolean anyFieldProvided = clientId != null || tenantId != null || scopes != null;
        if (anyFieldProvided == false) {
            return false;
        }

        var missingFields = new ArrayList<String>();
        if (clientId == null) {
            missingFields.add(CLIENT_ID_FIELD);
        }
        if (tenantId == null) {
            missingFields.add(TENANT_ID_FIELD);
        }
        if (scopes == null) {
            missingFields.add(SCOPES_FIELD);
        }

        if (missingFields.isEmpty() == false) {
            validationException.addValidationError(
                Strings.format(
                    "[%s] all OAuth2 fields must be provided together; missing: [%s]",
                    ModelConfigurations.SERVICE_SETTINGS,
                    String.join(", ", missingFields)
                )
            );
        }

        validationException.throwIfValidationErrorsExist();
        return true;
    }

    AzureOpenAiOAuthSettings(String clientId, String tenantId, List<String> scopes) {
        this.clientId = Objects.requireNonNull(clientId);
        this.tenantId = Objects.requireNonNull(tenantId);
        this.scopes = Objects.requireNonNull(scopes);
    }

    public AzureOpenAiOAuthSettings(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readStringCollectionAsImmutableList());
    }

    public String getClientId() {
        return clientId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public List<String> getScopes() {
        return scopes;
    }

    public AzureOpenAiOAuthSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();
        var updated = fromMapForUpdate(serviceSettings, validationException);

        var clientIdToUpdate = updated.clientId() != null ? updated.clientId() : clientId;
        var tenantIdToUpdate = updated.tenantId() != null ? updated.tenantId() : tenantId;
        var scopesToUpdate = updated.scopes() != null ? updated.scopes() : scopes;

        validateFields(clientIdToUpdate, tenantIdToUpdate, scopesToUpdate, validationException);

        return new AzureOpenAiOAuthSettings(clientIdToUpdate, tenantIdToUpdate, scopesToUpdate);
    }

    private static UpdateSettings fromMapForUpdate(Map<String, Object> map, ValidationException validationException) {
        var clientId = extractOptionalString(map, CLIENT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var tenant_id = extractOptionalString(map, TENANT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var scopes = extractStringList(map, SCOPES_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);

        validationException.throwIfValidationErrorsExist();

        return new UpdateSettings(clientId, tenant_id, scopes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(clientId);
        out.writeString(tenantId);
        out.writeStringCollection(scopes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CLIENT_ID_FIELD, clientId);
        builder.field(TENANT_ID_FIELD, tenantId);
        builder.field(SCOPES_FIELD, scopes);
        return builder;
    }

    public static Map<String, SettingsConfiguration> getClientSecretConfiguration() {
        return Map.of(
            AzureOpenAiOAuth2Secrets.CLIENT_SECRET_FIELD,
            new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.COMPLETION, TaskType.CHAT_COMPLETION))
                .setDescription(EXACTLY_ONE_CONFIG_DESCRIPTION)
                .setLabel("OAuth2 Client Secret")
                .setRequired(false)
                .setSensitive(true)
                .setUpdatable(true)
                .setType(SettingsConfigurationFieldType.STRING)
                .build()
        );
    }
}
