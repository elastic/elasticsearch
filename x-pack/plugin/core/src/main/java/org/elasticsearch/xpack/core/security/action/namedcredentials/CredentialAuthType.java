/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.namedcredentials;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The set of authentication schemes a named credential can hold. Each type declares which
 * field names are legal in the plaintext {@code config} block and the encrypted {@code auth}
 * block, so that requests can be validated without any per-field sensitivity decisions:
 * everything under {@code auth} is encrypted as a unit.
 */
public enum CredentialAuthType {
    BASIC("basic", Set.of(), Set.of("username", "password"), Set.of()),
    BEARER("bearer", Set.of(), Set.of("token"), Set.of()),
    API_KEY_HEADER("api_key_header", Set.of(), Set.of("api_key"), Set.of("header_field")),
    OAUTH_CLIENT_CREDENTIALS(
        "oauth_client_credentials",
        Set.of("token_url", "scope", "token_endpoint_auth_method"),
        Set.of("client_id", "client_secret"),
        Set.of()
    ),
    OAUTH_AUTHORIZATION_CODE(
        "oauth_authorization_code",
        Set.of("token_url", "authorization_url", "scope"),
        Set.of("client_id", "client_secret"),
        Set.of("access_token", "refresh_token")
    ),
    OAUTH_CLIENT_CREDENTIALS_PRIVATE_KEY_JWT(
        "oauth_client_credentials_private_key_jwt",
        Set.of("token_url", "scope", "algorithm", "certificate_binding", "key_id"),
        Set.of("client_id", "private_key"),
        Set.of("passphrase", "certificate")
    ),
    GCP_SERVICE_ACCOUNT("gcp_service_account", Set.of("scope"), Set.of("service_account_json"), Set.of()),
    AWS_CREDENTIALS("aws_credentials", Set.of(), Set.of("access_key_id", "secret_access_key"), Set.of());

    private final String typeName;
    private final Set<String> allowedConfigFields;
    private final Set<String> requiredAuthFields;
    private final Set<String> optionalAuthFields;

    CredentialAuthType(String typeName, Set<String> allowedConfigFields, Set<String> requiredAuthFields, Set<String> optionalAuthFields) {
        this.typeName = typeName;
        this.allowedConfigFields = allowedConfigFields;
        this.requiredAuthFields = requiredAuthFields;
        this.optionalAuthFields = optionalAuthFields;
    }

    public String typeName() {
        return typeName;
    }

    public static CredentialAuthType fromTypeName(String typeName) {
        for (CredentialAuthType type : values()) {
            if (type.typeName.equals(typeName)) {
                return type;
            }
        }
        throw new IllegalArgumentException(
            "unknown auth_type ["
                + typeName
                + "]; valid values are "
                + Arrays.stream(values()).map(CredentialAuthType::typeName).sorted().toList()
        );
    }

    /** Returns validation errors for the plaintext config block; empty means valid. */
    public List<String> validateConfig(Map<String, String> config) {
        List<String> errors = new ArrayList<>();
        for (String key : config.keySet().stream().sorted().toList()) {
            if (allowedConfigFields.contains(key) == false) {
                errors.add(
                    "unknown config field ["
                        + key
                        + "] for auth_type ["
                        + typeName
                        + "]; allowed fields are "
                        + allowedConfigFields.stream().sorted().toList()
                );
            }
        }
        return errors;
    }

    /** Returns validation errors for a fully-specified auth block; empty means valid. */
    public List<String> validateAuth(Map<String, String> auth) {
        List<String> errors = new ArrayList<>();
        for (String key : auth.keySet().stream().sorted().toList()) {
            if (requiredAuthFields.contains(key) == false && optionalAuthFields.contains(key) == false) {
                errors.add(
                    "unknown auth field ["
                        + key
                        + "] for auth_type ["
                        + typeName
                        + "]; allowed fields are "
                        + Stream.concat(requiredAuthFields.stream(), optionalAuthFields.stream()).sorted().toList()
                );
            } else {
                String value = auth.get(key);
                if (value == null || value.isEmpty()) {
                    errors.add("auth field [" + key + "] must not be empty for auth_type [" + typeName + "]");
                }
            }
        }
        for (String required : requiredAuthFields.stream().sorted().toList()) {
            if (auth.containsKey(required) == false) {
                errors.add("missing required auth field [" + required + "] for auth_type [" + typeName + "]");
            }
        }
        return errors;
    }
}
