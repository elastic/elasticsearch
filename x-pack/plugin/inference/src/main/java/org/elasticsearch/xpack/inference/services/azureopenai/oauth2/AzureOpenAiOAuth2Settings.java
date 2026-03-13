/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.oauth2;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

public class AzureOpenAiOAuth2Settings implements ToXContentFragment, Writeable {

    public static final TransportVersion AZURE_OPENAI_OAUTH_SETTINGS = TransportVersion.fromName("azure_openai_oauth_settings");

    public static final String TENANT_ID_FIELD = "tenant_id";

    public static final String REQUIRED_FIELDS = String.join(
        ", ",
        OAuth2Settings.CLIENT_ID_FIELD,
        TENANT_ID_FIELD,
        OAuth2Settings.SCOPES_FIELD
    );

    public static final String REQUIRED_FIELDS_DESCRIPTION = Strings.format("OAuth2 requires the fields [%s], to be set.", REQUIRED_FIELDS);

    private record UpdateSettings(OAuth2Settings oauth2Settings, @Nullable String tenantId) {
        UpdateSettings {
            Objects.requireNonNull(oauth2Settings);
        }
    }

    private final OAuth2Settings oauth2Settings;
    private final String tenantId;

    public static AzureOpenAiOAuth2Settings fromMap(Map<String, Object> map, ValidationException validationException) {
        var oauth2ServiceSettings = OAuth2Settings.fromMap(map, validationException);
        var tenantId = extractOptionalString(map, TENANT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);

        var hasFields = validateFields(oauth2ServiceSettings, tenantId, validationException);

        if (hasFields) {
            return new AzureOpenAiOAuth2Settings(oauth2ServiceSettings, tenantId);
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
        @Nullable OAuth2Settings oauth2Settings,
        @Nullable String tenantId,
        ValidationException validationException
    ) {
        boolean anyFieldProvided = oauth2Settings != null || tenantId != null;
        if (anyFieldProvided == false) {
            return false;
        }

        var missingFields = new ArrayList<String>();
        if (oauth2Settings == null) {
            missingFields.add(OAuth2Settings.REQUIRED_FIELDS);
        }
        if (tenantId == null) {
            missingFields.add(TENANT_ID_FIELD);
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

    public AzureOpenAiOAuth2Settings(OAuth2Settings oauth2Settings, String tenantId) {
        this.oauth2Settings = Objects.requireNonNull(oauth2Settings);
        this.tenantId = Objects.requireNonNull(tenantId);
    }

    public AzureOpenAiOAuth2Settings(StreamInput in) throws IOException {
        this.oauth2Settings = new OAuth2Settings(in);
        this.tenantId = in.readString();
    }

    public String getClientId() {
        return oauth2Settings.getClientId();
    }

    public String getTenantId() {
        return tenantId;
    }

    public List<String> getScopes() {
        return oauth2Settings.getScopes();
    }

    public AzureOpenAiOAuth2Settings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();
        var updated = fromMapForUpdate(serviceSettings, oauth2Settings, validationException);

        var tenantIdToUpdate = updated.tenantId() != null ? updated.tenantId() : tenantId;

        validateFields(updated.oauth2Settings(), tenantIdToUpdate, validationException);

        return new AzureOpenAiOAuth2Settings(updated.oauth2Settings(), tenantIdToUpdate);
    }

    private static UpdateSettings fromMapForUpdate(
        Map<String, Object> map,
        OAuth2Settings oAuth2Settings,
        ValidationException validationException
    ) {
        var oauth2ServiceSettings = oAuth2Settings.updateServiceSettings(map, validationException);
        var tenantId = extractOptionalString(map, TENANT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);

        validationException.throwIfValidationErrorsExist();

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

}
