/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Represents a request to create a token based on the provided information. This class accepts the
 * fields for an OAuth 2.0 access token request that uses the <code>password</code> grant type, the
 * <code>refresh_token</code> grant type, or one of the custom grant types such as
 * <code>_kerberos</code> and <code>_user_managed_service_account</code>.
 */
public final class CreateTokenRequest extends UntypedActionRequest {

    private static final TransportVersion UMSA_OAUTH2_TOKEN_EXCHANGE = TransportVersion.fromName("umsa_oauth2_token_exchange");

    public enum GrantType {
        PASSWORD("password"),
        KERBEROS("_kerberos"),
        REFRESH_TOKEN("refresh_token"),
        AUTHORIZATION_CODE("authorization_code"),
        CLIENT_CREDENTIALS("client_credentials"),
        USER_MANAGED_SERVICE_ACCOUNT("_user_managed_service_account");

        private final String value;

        GrantType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static GrantType fromString(String grantType) {
            if (grantType != null) {
                for (GrantType type : values()) {
                    if (type.getValue().equals(grantType)) {
                        return type;
                    }
                }
            }
            return null;
        }
    }

    private static final Set<GrantType> SUPPORTED_GRANT_TYPES = Collections.unmodifiableSet(
        EnumSet.of(
            GrantType.PASSWORD,
            GrantType.KERBEROS,
            GrantType.REFRESH_TOKEN,
            GrantType.CLIENT_CREDENTIALS,
            GrantType.USER_MANAGED_SERVICE_ACCOUNT
        )
    );

    private String grantType;
    private String username;
    private SecureString password;
    private SecureString kerberosTicket;
    private String scope;
    private String refreshToken;
    private SecureString serviceAccountToken;

    public CreateTokenRequest(StreamInput in) throws IOException {
        super(in);
        grantType = in.readString();
        username = in.readOptionalString();
        password = in.readOptionalSecureString();
        refreshToken = in.readOptionalString();
        scope = in.readOptionalString();
        kerberosTicket = in.readOptionalSecureString();
        if (in.getTransportVersion().supports(UMSA_OAUTH2_TOKEN_EXCHANGE)) {
            serviceAccountToken = in.readOptionalSecureString();
        }
    }

    public CreateTokenRequest() {}

    public CreateTokenRequest(
        String grantType,
        @Nullable String username,
        @Nullable SecureString password,
        @Nullable SecureString kerberosTicket,
        @Nullable String scope,
        @Nullable String refreshToken
    ) {
        this(grantType, username, password, kerberosTicket, scope, refreshToken, null);
    }

    public CreateTokenRequest(
        String grantType,
        @Nullable String username,
        @Nullable SecureString password,
        @Nullable SecureString kerberosTicket,
        @Nullable String scope,
        @Nullable String refreshToken,
        @Nullable SecureString serviceAccountToken
    ) {
        this.grantType = grantType;
        this.username = username;
        this.password = password;
        this.kerberosTicket = kerberosTicket;
        this.scope = scope;
        this.refreshToken = refreshToken;
        this.serviceAccountToken = serviceAccountToken;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        GrantType type = GrantType.fromString(grantType);
        if (type != null) {
            switch (type) {
                case PASSWORD -> {
                    validationException = validateUnsupportedField(type, "kerberos_ticket", kerberosTicket, validationException);
                    validationException = validateUnsupportedField(type, "refresh_token", refreshToken, validationException);
                    validationException = validateUnsupportedField(type, "service_account_token", serviceAccountToken, validationException);
                    validationException = validateRequiredField("username", username, validationException);
                    validationException = validateRequiredField("password", password, validationException);
                }
                case KERBEROS -> {
                    validationException = validateUnsupportedField(type, "username", username, validationException);
                    validationException = validateUnsupportedField(type, "password", password, validationException);
                    validationException = validateUnsupportedField(type, "refresh_token", refreshToken, validationException);
                    validationException = validateUnsupportedField(type, "service_account_token", serviceAccountToken, validationException);
                    validationException = validateRequiredField("kerberos_ticket", kerberosTicket, validationException);
                }
                case REFRESH_TOKEN -> {
                    validationException = validateUnsupportedField(type, "username", username, validationException);
                    validationException = validateUnsupportedField(type, "password", password, validationException);
                    validationException = validateUnsupportedField(type, "kerberos_ticket", kerberosTicket, validationException);
                    validationException = validateUnsupportedField(type, "service_account_token", serviceAccountToken, validationException);
                    validationException = validateRequiredField("refresh_token", refreshToken, validationException);
                }
                case CLIENT_CREDENTIALS -> {
                    validationException = validateUnsupportedField(type, "username", username, validationException);
                    validationException = validateUnsupportedField(type, "password", password, validationException);
                    validationException = validateUnsupportedField(type, "kerberos_ticket", kerberosTicket, validationException);
                    validationException = validateUnsupportedField(type, "refresh_token", refreshToken, validationException);
                    validationException = validateUnsupportedField(type, "service_account_token", serviceAccountToken, validationException);
                }
                case USER_MANAGED_SERVICE_ACCOUNT -> {
                    validationException = validateUnsupportedField(type, "username", username, validationException);
                    validationException = validateUnsupportedField(type, "password", password, validationException);
                    validationException = validateUnsupportedField(type, "kerberos_ticket", kerberosTicket, validationException);
                    validationException = validateUnsupportedField(type, "refresh_token", refreshToken, validationException);
                    validationException = validateRequiredField("service_account_token", serviceAccountToken, validationException);
                }
                default -> validationException = addValidationError(
                    "grant_type only supports the values: ["
                        + SUPPORTED_GRANT_TYPES.stream().map(GrantType::getValue).collect(Collectors.joining(", "))
                        + "]",
                    validationException
                );
            }
        } else {
            validationException = addValidationError(
                "grant_type only supports the values: ["
                    + SUPPORTED_GRANT_TYPES.stream().map(GrantType::getValue).collect(Collectors.joining(", "))
                    + "]",
                validationException
            );
        }
        return validationException;
    }

    private static ActionRequestValidationException validateRequiredField(
        String field,
        String fieldValue,
        ActionRequestValidationException validationException
    ) {
        if (Strings.isNullOrEmpty(fieldValue)) {
            validationException = addValidationError(String.format(Locale.ROOT, "%s is missing", field), validationException);
        }
        return validationException;
    }

    private static ActionRequestValidationException validateRequiredField(
        String field,
        SecureString fieldValue,
        ActionRequestValidationException validationException
    ) {
        if (fieldValue == null || fieldValue.getChars() == null || fieldValue.length() == 0) {
            validationException = addValidationError(String.format(Locale.ROOT, "%s is missing", field), validationException);
        }
        return validationException;
    }

    private static ActionRequestValidationException validateUnsupportedField(
        GrantType grantType,
        String field,
        Object fieldValue,
        ActionRequestValidationException validationException
    ) {
        if (fieldValue != null) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "%s is not supported with the %s grant_type", field, grantType.getValue()),
                validationException
            );
        }
        return validationException;
    }

    public void setGrantType(String grantType) {
        this.grantType = grantType;
    }

    public void setUsername(@Nullable String username) {
        this.username = username;
    }

    public void setPassword(@Nullable SecureString password) {
        this.password = password;
    }

    public void setKerberosTicket(@Nullable SecureString kerberosTicket) {
        this.kerberosTicket = kerberosTicket;
    }

    public void setScope(@Nullable String scope) {
        this.scope = scope;
    }

    public void setRefreshToken(@Nullable String refreshToken) {
        this.refreshToken = refreshToken;
    }

    public void setServiceAccountToken(@Nullable SecureString serviceAccountToken) {
        this.serviceAccountToken = serviceAccountToken;
    }

    public String getGrantType() {
        return grantType;
    }

    @Nullable
    public String getUsername() {
        return username;
    }

    @Nullable
    public SecureString getPassword() {
        return password;
    }

    @Nullable
    public SecureString getKerberosTicket() {
        return kerberosTicket;
    }

    @Nullable
    public String getScope() {
        return scope;
    }

    @Nullable
    public String getRefreshToken() {
        return refreshToken;
    }

    @Nullable
    public SecureString getServiceAccountToken() {
        return serviceAccountToken;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(grantType);
        out.writeOptionalString(username);
        out.writeOptionalSecureString(password);
        out.writeOptionalString(refreshToken);
        out.writeOptionalString(scope);
        out.writeOptionalSecureString(kerberosTicket);
        if (out.getTransportVersion().supports(UMSA_OAUTH2_TOKEN_EXCHANGE)) {
            out.writeOptionalSecureString(serviceAccountToken);
        } else if (serviceAccountToken != null) {
            // Never silently drop a credential: an older node would fail the grant type validation with a
            // misleading message, so fail the serialization instead.
            throw new IllegalArgumentException(
                "versions of Elasticsearch before ["
                    + UMSA_OAUTH2_TOKEN_EXCHANGE.toReleaseVersion()
                    + "] can't handle the [_user_managed_service_account] grant type and attempted to send to ["
                    + out.getTransportVersion().toReleaseVersion()
                    + "]"
            );
        }
    }
}
