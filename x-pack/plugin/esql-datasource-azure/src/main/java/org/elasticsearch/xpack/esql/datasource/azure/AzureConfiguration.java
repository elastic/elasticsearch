/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceConfiguration;

import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.plaintext;
import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.secret;

/**
 * Configuration for Azure Blob Storage access including credentials and endpoint settings.
 * <p>
 * Supports authentication modes:
 * <ul>
 *   <li>Connection string (full connection string)</li>
 *   <li>Account + key (SharedKey auth)</li>
 *   <li>SAS token</li>
 *   <li>Workload identity federation via {@code tenant_id}, {@code client_id}, and {@code jwt_audience}</li>
 *   <li>{@code auth=none} for anonymous access to public containers</li>
 *   <li>{@code auth=workload_identity} to use the node's managed identity via Azure IMDS. Requires the
 *       {@code esql.datasource.workload_identity.enabled} cluster setting.</li>
 * </ul>
 */
public class AzureConfiguration extends FileDataSourceConfiguration {

    private static final DataSourceConfigDefinition CONNECTION_STRING = secret("connection_string");
    private static final DataSourceConfigDefinition ACCOUNT = plaintext("account");
    private static final DataSourceConfigDefinition KEY = secret("key");
    private static final DataSourceConfigDefinition SAS_TOKEN = secret("sas_token");
    private static final DataSourceConfigDefinition ENDPOINT = plaintext("endpoint");
    private static final DataSourceConfigDefinition TENANT_ID = plaintext("tenant_id").asKeylessAuth();
    private static final DataSourceConfigDefinition CLIENT_ID = plaintext("client_id").asKeylessAuth();
    private static final DataSourceConfigDefinition JWT_AUDIENCE = plaintext("jwt_audience").asKeylessAuth();

    private static final Map<String, DataSourceConfigDefinition> FIELDS = DataSourceConfigDefinition.mapOf(
        CONNECTION_STRING,
        ACCOUNT,
        KEY,
        SAS_TOKEN,
        ENDPOINT,
        TENANT_ID,
        CLIENT_ID,
        JWT_AUDIENCE,
        AUTH
    );

    private AzureConfiguration(Map<String, Object> raw) {
        super(raw, FIELDS);
    }

    @Override
    protected void validateCredentials(ValidationException errors) {
        if (hasKeylessAuth()) {
            if (tenantId() == null) {
                errors.addValidationError("tenant_id is required when keyless authentication settings are configured");
            }
            if (clientId() == null) {
                errors.addValidationError("client_id is required when keyless authentication settings are configured");
            }
            if (jwtAudience() == null) {
                errors.addValidationError("jwt_audience is required when keyless authentication settings are configured");
            }
        }
    }

    public static AzureConfiguration fromMap(Map<String, Object> raw) {
        return raw == null || raw.isEmpty() ? null : new AzureConfiguration(raw);
    }

    /**
     * Lenient factory for query-time configuration maps, which may carry format-level options
     * (e.g. {@code header_row}) alongside storage-level options. Filters unknown keys
     * before construction; cross-field validation (auth/credential conflicts) still runs.
     */
    public static Configured<AzureConfiguration> fromQueryConfig(Map<String, Object> raw) {
        return filterAndConstruct(raw, FIELDS, AzureConfiguration::new);
    }

    public static AzureConfiguration fromFields(String connectionString, String account, String key, String sasToken, String endpoint) {
        return fromFields(connectionString, account, key, sasToken, endpoint, null);
    }

    public static AzureConfiguration fromFields(
        String connectionString,
        String account,
        String key,
        String sasToken,
        String endpoint,
        String auth
    ) {
        var raw = buildRawMap(
            CONNECTION_STRING,
            connectionString,
            ACCOUNT,
            account,
            KEY,
            key,
            SAS_TOKEN,
            sasToken,
            ENDPOINT,
            endpoint,
            AUTH,
            auth
        );
        return raw != null ? fromMap(raw) : null;
    }

    public String connectionString() {
        return get(CONNECTION_STRING.name());
    }

    public String account() {
        return get(ACCOUNT.name());
    }

    public String key() {
        return get(KEY.name());
    }

    public String sasToken() {
        return get(SAS_TOKEN.name());
    }

    public String endpoint() {
        return get(ENDPOINT.name());
    }

    /**
     * Azure AD tenant ID configured on {@code com.azure.identity.ClientAssertionCredential} for keyless
     * workload-identity federation.
     */
    public String tenantId() {
        return get(TENANT_ID.name());
    }

    /**
     * Client ID of the Azure AD application (or user-assigned managed identity) whose federated identity
     * credential trusts the workload-identity issuer; configured on {@code ClientAssertionCredential}.
     */
    public String clientId() {
        return get(CLIENT_ID.name());
    }

    /**
     * Audience passed to the workload-identity issuer {@code IssueTokenRequest} when minting the JWT that
     * is presented to Azure AD as a client assertion (typically {@code api://AzureADTokenExchange}).
     */
    public String jwtAudience() {
        return get(JWT_AUDIENCE.name());
    }

    public boolean hasCredentials() {
        return hasExplicitCredentials();
    }

    private boolean hasExplicitCredentials() {
        return Strings.hasText(connectionString()) || (Strings.hasText(account()) && Strings.hasText(key())) || Strings.hasText(sasToken());
    }
}
