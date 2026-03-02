/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.oauth;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StringParser.extractStringList;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

public class AzureOpenAiOAuthSettings2 implements ToXContentFragment, Writeable {
    public static final String CLIENT_ID_FIELD = "client_id";
    public static final String TENANT_ID_FIELD = "tenant_id";
    public static final String SCOPES_FIELD = "scopes";

    private record UpdateSettings(@Nullable String clientId, @Nullable String tenantId, @Nullable List<String> scopes) {}

    private final String clientId;
    private final String tenantId;
    private final List<String> scopes;

    public static AzureOpenAiOAuthSettings2 fromMap(Map<String, Object> map, ValidationException validationException) {
        var clientId = extractOptionalString(map, CLIENT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var tenantId = extractOptionalString(map, TENANT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var scopes = extractStringList(map, SCOPES_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);

        validateFields(clientId, tenantId, scopes, validationException);

        return new AzureOpenAiOAuthSettings2(clientId, tenantId, scopes);
    }

    private static void validateFields(
        @Nullable String clientId,
        @Nullable String tenantId,
        @Nullable List<String> scopes,
        ValidationException validationException
    ) {
        boolean anyFieldProvided = clientId != null || tenantId != null || scopes != null;
        boolean allFieldsProvided = clientId != null && tenantId != null && scopes != null;

        if (anyFieldProvided && allFieldsProvided == false) {
            validationException.addValidationError(
                Strings.format(
                    "[%s] all oauth fields must be provided together, received client_id=%s tenant_id=%s scopes=%s",
                    ModelConfigurations.SERVICE_SETTINGS,
                    clientId,
                    tenantId,
                    scopes
                )
            );
        }

        validationException.throwIfValidationErrorsExist();
    }

    AzureOpenAiOAuthSettings2(String clientId, String tenantId, List<String> scopes) {
        this.clientId = Objects.requireNonNull(clientId);
        this.tenantId = Objects.requireNonNull(tenantId);
        this.scopes = Objects.requireNonNull(scopes);
    }

    public AzureOpenAiOAuthSettings2(StreamInput in) throws IOException {
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

    public AzureOpenAiOAuthSettings2 updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();
        var updated = fromMapForUpdate(serviceSettings, validationException);

        var clientIdToUpdate = updated.clientId() != null ? updated.clientId() : clientId;
        var tenantIdToUpdate = updated.tenantId() != null ? updated.tenantId() : tenantId;
        var scopesToUpdate = updated.scopes() != null ? updated.scopes() : scopes;

        validateFields(clientIdToUpdate, tenantIdToUpdate, scopesToUpdate, validationException);

        // It is not possible to remove a field because all fields are required, so a user can only update existing fields
        return new AzureOpenAiOAuthSettings2(clientIdToUpdate, tenantIdToUpdate, scopesToUpdate);
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
}
