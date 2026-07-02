/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceConfiguration;

import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.plaintext;
import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.secret;

/**
 * Configuration for Google Cloud Storage access including credentials and endpoint settings.
 * <p>
 * The {@code auth} setting selects the mode explicitly — {@code auto}, {@code anonymous},
 * {@code static_credentials}, {@code federated_identity}, or {@code managed_identity}. When omitted it defaults to
 * {@code auto}, which infers the mode from the fields present. Supported modes:
 * <ul>
 *   <li>{@code auth=static_credentials} — service account JSON credentials (inline) or a short-lived OAuth2 access token</li>
 *   <li>{@code auth=federated_identity} — workload identity federation via {@code jwt_audience}, {@code sts_audience},
 *       and {@code service_account_impersonation_url}</li>
 *   <li>{@code auth=anonymous} — anonymous access to public buckets</li>
 *   <li>{@code auth=managed_identity} — the node's own GCE/GKE metadata-server credentials,
 *       gated by the {@code esql.datasource.managed_identity.enabled} cluster setting</li>
 * </ul>
 * Apart from {@code auth=managed_identity}, a data source must carry its own credentials, since the node may run
 * in a different cloud than the bucket it targets. {@code auth=managed_identity} is the deliberate exception: it
 * is intended for single-cloud, single-tenant deployments where the node's metadata-server credentials are the
 * intended identity, which is why it is disabled by default.
 */
public class GcsConfiguration extends FileDataSourceConfiguration {

    private static final DataSourceConfigDefinition CREDENTIALS = secret("credentials");
    private static final DataSourceConfigDefinition ACCESS_TOKEN = secret("access_token");
    private static final DataSourceConfigDefinition PROJECT_ID = plaintext("project_id");
    private static final DataSourceConfigDefinition ENDPOINT = plaintext("endpoint");
    private static final DataSourceConfigDefinition TOKEN_URI = plaintext("token_uri");
    private static final DataSourceConfigDefinition JWT_AUDIENCE = plaintext("jwt_audience").asKeylessAuth();
    private static final DataSourceConfigDefinition STS_AUDIENCE = plaintext("sts_audience").asKeylessAuth();
    private static final DataSourceConfigDefinition SERVICE_ACCOUNT_IMPERSONATION_URL = plaintext("service_account_impersonation_url")
        .asKeylessAuth();

    private static final Map<String, DataSourceConfigDefinition> FIELDS = DataSourceConfigDefinition.mapOf(
        CREDENTIALS,
        ACCESS_TOKEN,
        PROJECT_ID,
        ENDPOINT,
        TOKEN_URI,
        JWT_AUDIENCE,
        STS_AUDIENCE,
        SERVICE_ACCOUNT_IMPERSONATION_URL,
        AUTH
    );

    private GcsConfiguration(Map<String, Object> raw) {
        super(raw, FIELDS);
    }

    @Override
    protected void validateCredentials(ValidationException errors) {
        // service_account_impersonation_url is optional: direct workload-identity federation maps the
        // federated identity straight to a principal without impersonating a service account.
        if (hasKeylessAuth()) {
            if (jwtAudience() == null) {
                errors.addValidationError("jwt_audience is required when keyless authentication settings are configured");
            }
            if (stsAudience() == null) {
                errors.addValidationError("sts_audience is required when keyless authentication settings are configured");
            }
        }
    }

    public static GcsConfiguration fromMap(Map<String, Object> raw) {
        return raw == null || raw.isEmpty() ? null : new GcsConfiguration(raw);
    }

    /**
     * Lenient factory for query-time configuration maps, which may carry format-level options
     * (e.g. {@code header_row}) alongside storage-level options. Filters unknown keys
     * before construction; cross-field validation (auth/credential conflicts) still runs.
     */
    public static Configured<GcsConfiguration> fromQueryConfig(Map<String, Object> raw) {
        return filterAndConstruct(raw, FIELDS, GcsConfiguration::new);
    }

    public static GcsConfiguration fromFields(String serviceAccountCredentials, String projectId, String endpoint) {
        return fromFields(serviceAccountCredentials, projectId, endpoint, null, null);
    }

    public static GcsConfiguration fromFields(String serviceAccountCredentials, String projectId, String endpoint, String tokenUri) {
        return fromFields(serviceAccountCredentials, projectId, endpoint, tokenUri, null);
    }

    public static GcsConfiguration fromFields(
        String serviceAccountCredentials,
        String projectId,
        String endpoint,
        String tokenUri,
        String auth
    ) {
        return fromFields(serviceAccountCredentials, projectId, endpoint, tokenUri, auth, null, null, null);
    }

    public static GcsConfiguration fromFields(
        String serviceAccountCredentials,
        String projectId,
        String endpoint,
        String tokenUri,
        String auth,
        String jwtAudience,
        String stsAudience,
        String serviceAccountImpersonationUrl
    ) {
        var raw = buildRawMap(
            CREDENTIALS,
            serviceAccountCredentials,
            PROJECT_ID,
            projectId,
            ENDPOINT,
            endpoint,
            TOKEN_URI,
            tokenUri,
            JWT_AUDIENCE,
            jwtAudience,
            STS_AUDIENCE,
            stsAudience,
            SERVICE_ACCOUNT_IMPERSONATION_URL,
            serviceAccountImpersonationUrl,
            AUTH,
            auth
        );
        return raw != null ? fromMap(raw) : null;
    }

    public String serviceAccountCredentials() {
        return get(CREDENTIALS.name());
    }

    public String accessToken() {
        return get(ACCESS_TOKEN.name());
    }

    public String projectId() {
        return get(PROJECT_ID.name());
    }

    public String endpoint() {
        return get(ENDPOINT.name());
    }

    public String tokenUri() {
        return get(TOKEN_URI.name());
    }

    /**
     * Audience passed to the workload-identity issuer {@code IssueTokenRequest} when minting a JWT.
     */
    public String jwtAudience() {
        return get(JWT_AUDIENCE.name());
    }

    /**
     * Audience configured on {@code com.google.auth.oauth2.IdentityPoolCredentials} when exchanging
     * the workload-identity JWT with Google STS.
     */
    public String stsAudience() {
        return get(STS_AUDIENCE.name());
    }

    /**
     * Optional service account impersonation URL configured on {@code com.google.auth.oauth2.IdentityPoolCredentials}.
     * When {@code null}, the federated identity maps directly to a principal without impersonating a service account.
     */
    public String serviceAccountImpersonationUrl() {
        return get(SERVICE_ACCOUNT_IMPERSONATION_URL.name());
    }

    @Override
    public boolean hasCredentials() {
        return Strings.hasText(serviceAccountCredentials()) || Strings.hasText(accessToken());
    }

    @Override
    public String unresolvedAuthMessage() {
        return "GCS data source requires credentials: set credentials (a service-account JSON key) "
            + "or access_token for short-lived OAuth credentials; "
            + "set auth=anonymous for public buckets; "
            + "set auth=managed_identity to use the node's metadata-server credentials "
            + "(requires the esql.datasource.managed_identity.enabled cluster setting); "
            + "or configure keyless authentication with jwt_audience and sts_audience";
    }
}
