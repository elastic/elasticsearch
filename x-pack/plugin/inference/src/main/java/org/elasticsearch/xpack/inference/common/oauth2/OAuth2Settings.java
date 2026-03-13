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
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
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

    private record UpdateSettings(@Nullable String clientId, @Nullable List<String> scopes) {}

    private final String clientId;
    private final List<String> scopes;

    /**
     * Parses client_id and scopes from the map. Either both must be present or both absent.
     *
     * @return a new instance if both fields are present, null if both are absent
     */
    public static OAuth2Settings fromMap(Map<String, Object> map, ValidationException validationException) {
        var clientId = extractOptionalString(map, CLIENT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var scopes = extractStringList(map, SCOPES_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);

        var hasFields = validateFields(clientId, scopes, validationException);

        if (hasFields) {
            return new OAuth2Settings(clientId, scopes);
        }

        return null;
    }

    /**
     * Validates that either both or neither of client_id and scopes are provided.
     *
     * @return true if both are provided, false if neither is provided
     */
    private static boolean validateFields(
        @Nullable String clientId,
        @Nullable List<String> scopes,
        ValidationException validationException
    ) {
        boolean anyFieldProvided = clientId != null || scopes != null;
        if (anyFieldProvided == false) {
            return false;
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
        }

        validationException.throwIfValidationErrorsExist();
        return true;
    }

    OAuth2Settings(String clientId, List<String> scopes) {
        this.clientId = Objects.requireNonNull(clientId);
        this.scopes = Objects.requireNonNull(scopes);
    }

    public OAuth2Settings(StreamInput in) throws IOException {
        this(in.readString(), in.readStringCollectionAsImmutableList());
    }

    public String getClientId() {
        return clientId;
    }

    public List<String> getScopes() {
        return scopes;
    }

    public OAuth2Settings updateServiceSettings(Map<String, Object> map, ValidationException validationException) {
        var updated = fromMapForUpdate(map, validationException);

        var clientIdToUpdate = updated.clientId() != null ? updated.clientId() : clientId;
        var scopesToUpdate = updated.scopes() != null ? updated.scopes() : scopes;

        validateFields(clientIdToUpdate, scopesToUpdate, validationException);

        return new OAuth2Settings(clientIdToUpdate, scopesToUpdate);
    }

    private static UpdateSettings fromMapForUpdate(Map<String, Object> map, ValidationException validationException) {
        var clientId = extractOptionalString(map, CLIENT_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var scopes = extractStringList(map, SCOPES_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);

        validationException.throwIfValidationErrorsExist();

        return new UpdateSettings(clientId, scopes);
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
}
