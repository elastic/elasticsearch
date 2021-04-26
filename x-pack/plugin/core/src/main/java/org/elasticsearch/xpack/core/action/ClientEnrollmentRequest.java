/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class ClientEnrollmentRequest extends ActionRequest {

    public enum ClientType {
        KIBANA("kibana"), GENERIC("generic_client");

        private final String value;

        ClientType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static ClientEnrollmentRequest.ClientType fromString(String clientType) {
            if (clientType != null) {
                for (ClientEnrollmentRequest.ClientType type : values()) {
                    if (type.getValue().equals(clientType)) {
                        return type;
                    }
                }
            }
            return null;
        }
    }

    public static final Set<ClientType> SUPPORTED_CLIENT_TYPES = Set.of(ClientType.KIBANA, ClientType.GENERIC);

    private String clientType;
    private SecureString clientPassword;

    public ClientEnrollmentRequest() {
    }

    public ClientEnrollmentRequest(String clientType, @Nullable SecureString clientPassword) {
        this.clientType = Objects.requireNonNull(clientType);
        this.clientPassword = clientPassword;
    }

    public ClientEnrollmentRequest(StreamInput in) throws IOException {
        super(in);
        clientType = in.readString();
        clientPassword = in.readOptionalSecureString();
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        final ClientType type = ClientType.fromString(clientType);
        if (null != type) {
            if (type.equals(ClientType.GENERIC)) {
                if (null != clientPassword) {
                    validationException =
                        addValidationError("client_password cannot be set when client_type is " + ClientType.GENERIC.value,
                            validationException);
                }
            } else if (type.equals(ClientType.KIBANA) == false) {
                validationException = addValidationError("client_type only supports the values: [" + SUPPORTED_CLIENT_TYPES.stream()
                    .map(ClientType::getValue)
                    .collect(Collectors.joining(", ")) + "]", validationException);
            }
        } else {
            validationException = addValidationError("client_type needs to be set", validationException);
        }
        return validationException;
    }

    public String getClientType() {
        return clientType;
    }

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }

    public SecureString getClientPassword() {
        return clientPassword;
    }

    public void setClientPassword(SecureString clientPassword) {
        this.clientPassword = clientPassword;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(clientType);
        out.writeOptionalSecureString(clientPassword);
    }
}
