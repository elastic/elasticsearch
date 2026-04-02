/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StringParser.extractStringList;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

/**
 * Holds OAuth2 service-level settings: client ID and scopes.
 */
public class OAuth2Settings implements ToXContentFragment, Writeable {

    public static final String CLIENT_ID_FIELD = "client_id";
    public static final String SCOPES_FIELD = "scopes";

    public static final String REQUIRED_FIELDS = String.join(", ", CLIENT_ID_FIELD, SCOPES_FIELD);

    private static final String CLIENT_ID_CONFIG_DESCRIPTION = "ID of application registered with the authorization server.";
    private static final String SCOPES_CONFIG_DESCRIPTION = "The permissions that the application is requesting.";

    private final String clientId;
    private final List<String> scopes;

    /**
     * Parses client_id and scopes from the map. Either both must be present or both absent.
     *
     * @return {@link ValidationResult} with the created {@link OAuth2Settings} object if both client_id and scopes are provided,
     * {@link ValidationResult#undefined()} if both are absent, or {@link ValidationResult#failed()} if only one is provided
     * (with a validation error added to the exception)
     */
    public static ValidationResult<OAuth2Settings> fromMap(Map<String, Object> map, ValidationException validationException) {
        var clientId = extractOptionalString(map, CLIENT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var scopes = extractStringList(map, SCOPES_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);

        return validateFields(clientId, scopes, validationException);
    }

    /**
     * Validates that either both or neither of client_id and scopes are provided.
     *
     * @return {@link ValidationResult} with the created {@link OAuth2Settings} object if both client_id and scopes are provided,
     * {@link ValidationResult#undefined()} if both are absent, or {@link ValidationResult#failed()} if only one is provided
     * (with a validation error added to the exception)
     */
    private static ValidationResult<OAuth2Settings> validateFields(
        @Nullable String clientId,
        @Nullable List<String> scopes,
        ValidationException validationException
    ) {
        var allFieldsMissing = clientId == null && scopes == null;

        if (allFieldsMissing) {
            return ValidationResult.undefined();
        }

        var missingFields = new ArrayList<String>();
        if (clientId == null) {
            missingFields.add(CLIENT_ID_FIELD);
        }
        if (scopes == null) {
            missingFields.add(SCOPES_FIELD);
        }

        if (missingFields.isEmpty() == false) {
            validationException.addValidationError(
                Strings.format(
                    "[%s] OAuth2 fields [%s] must be provided together; missing: [%s]",
                    ModelConfigurations.SERVICE_SETTINGS,
                    REQUIRED_FIELDS,
                    String.join(", ", missingFields)
                )
            );
            return ValidationResult.failed();
        }

        return ValidationResult.success(new OAuth2Settings(clientId, scopes));
    }

    public static boolean hasAnyOAuth2Fields(Map<String, Object> map) {
        return map.containsKey(CLIENT_ID_FIELD) || map.containsKey(SCOPES_FIELD);
    }

    public OAuth2Settings(String clientId, List<String> scopes) {
        this.clientId = Objects.requireNonNull(clientId);
        this.scopes = Objects.requireNonNull(scopes);
    }

    public OAuth2Settings(StreamInput in) throws IOException {
        this(in.readString(), in.readStringCollectionAsImmutableList());
    }

    public String clientId() {
        return clientId;
    }

    public List<String> scopes() {
        return scopes;
    }

    /**
     * Updates the current settings with any new values provided in the map.
     * If a field is not present in the map, the existing value is retained.
     * @param serviceSettingsMap the map containing the new settings values
     * @param validationException the exception to which any validation errors should be added
     * @return a new {@link OAuth2Settings} object with the updated values, or the existing values if not updated
     */
    public OAuth2Settings updateServiceSettings(Map<String, Object> serviceSettingsMap, ValidationException validationException) {
        var extractedClientId = extractOptionalString(
            serviceSettingsMap,
            CLIENT_ID_FIELD,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var extractedScopes = extractStringList(
            serviceSettingsMap,
            SCOPES_FIELD,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        return new OAuth2Settings(
            extractedClientId != null ? extractedClientId : this.clientId,
            extractedScopes != null ? extractedScopes : this.scopes
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(clientId);
        out.writeStringCollection(scopes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CLIENT_ID_FIELD, clientId);
        builder.field(SCOPES_FIELD, scopes);
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
        OAuth2Settings that = (OAuth2Settings) o;
        return Objects.equals(clientId, that.clientId) && Objects.equals(scopes, that.scopes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, scopes);
    }

    public static Map<String, SettingsConfiguration> getOAuth2Configurations(EnumSet<TaskType> taskTypes) {
        return Map.of(
            CLIENT_ID_FIELD,
            new SettingsConfiguration.Builder(taskTypes).setDescription(CLIENT_ID_CONFIG_DESCRIPTION)
                .setLabel("OAuth2 Client ID")
                .setRequired(false)
                .setSensitive(false)
                .setUpdatable(true)
                .setType(SettingsConfigurationFieldType.STRING)
                .build(),
            SCOPES_FIELD,
            new SettingsConfiguration.Builder(taskTypes).setDescription(SCOPES_CONFIG_DESCRIPTION)
                .setLabel("OAuth2 Scopes")
                .setRequired(false)
                .setSensitive(false)
                .setUpdatable(true)
                .setType(SettingsConfigurationFieldType.LIST)
                .build()
        );
    }
}
