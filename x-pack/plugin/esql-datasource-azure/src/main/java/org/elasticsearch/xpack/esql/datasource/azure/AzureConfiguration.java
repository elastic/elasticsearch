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
 * The {@code auth} setting selects the mode explicitly — {@code auto}, {@code anonymous},
 * {@code static_credentials}, {@code federated_identity}, or {@code managed_identity}. When omitted it defaults to
 * {@code auto}, which infers the mode from the fields present. Supported modes:
 * <ul>
 *   <li>{@code auth=static_credentials} — a connection string, account + key (SharedKey auth), or account + SAS token</li>
 *   <li>{@code auth=federated_identity} — workload identity federation via {@code tenant_id}, {@code client_id},
 *       and {@code jwt_audience}</li>
 *   <li>{@code auth=anonymous} — anonymous access to public containers</li>
 *   <li>{@code auth=managed_identity} — the node's managed identity via Azure IMDS. Requires the
 *       {@code esql.datasource.managed_identity.enabled} cluster setting.</li>
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

    @Override
    public boolean hasCredentials() {
        return hasExplicitCredentials();
    }

    @Override
    public String unresolvedAuthMessage() {
        return "Azure data source requires credentials: set connection_string, or account and key, "
            + "or account and sas_token; "
            + "set auth=anonymous for public containers; "
            + "set auth=managed_identity to use the node's managed identity "
            + "(requires the esql.datasource.managed_identity.enabled cluster setting); "
            + "or configure keyless authentication with tenant_id, client_id, and jwt_audience";
    }

    private boolean hasExplicitCredentials() {
        // Every form the STATIC_CREDENTIALS provider arm can actually build: a full connection_string, account+key,
        // or account+sas_token. sas_token alone (no account) is NOT a complete credential — requiring account here
        // means validate() rejects it up front instead of letting it resolve to static and fail at query time.
        return Strings.hasText(connectionString())
            || (Strings.hasText(account()) && Strings.hasText(key()))
            || (Strings.hasText(account()) && Strings.hasText(sasToken()));
    }
}
