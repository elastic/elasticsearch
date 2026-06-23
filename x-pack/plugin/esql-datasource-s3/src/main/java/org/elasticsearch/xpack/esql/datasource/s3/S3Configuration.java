/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceConfiguration;

import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.plaintext;
import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.secret;

/**
 * Configuration for S3 access including credentials and endpoint settings.
 * <p>
 * Supports authentication modes:
 * <ul>
 *   <li>Access key + secret key (static credentials)</li>
 *   <li>Access key + secret key + session token (STS temporary credentials)</li>
 *   <li>Keyless workload-identity federation via {@code role_arn} and {@code jwt_audience}
 *       (optionally {@code role_session_name} and {@code sts_endpoint})</li>
 *   <li>{@code auth=none} for anonymous access to public buckets</li>
 *   <li>{@code auth=workload_identity} to use the node's instance credentials via the IMDS-family chain
 *       (ECS task role, then EC2 instance profile). Requires the
 *       {@code esql.datasource.workload_identity.enabled} cluster setting.</li>
 * </ul>
 */
public class S3Configuration extends FileDataSourceConfiguration {

    private static final DataSourceConfigDefinition ACCESS_KEY = secret("access_key");
    private static final DataSourceConfigDefinition SECRET_KEY = secret("secret_key");
    private static final DataSourceConfigDefinition SESSION_TOKEN = secret("session_token");
    private static final DataSourceConfigDefinition ENDPOINT = plaintext("endpoint");
    private static final DataSourceConfigDefinition REGION = plaintext("region");
    private static final DataSourceConfigDefinition ROLE_ARN = plaintext("role_arn").asKeylessAuth();
    private static final DataSourceConfigDefinition ROLE_SESSION_NAME = plaintext("role_session_name").asKeylessAuth();
    private static final DataSourceConfigDefinition JWT_AUDIENCE = plaintext("jwt_audience").asKeylessAuth();
    private static final DataSourceConfigDefinition STS_ENDPOINT = plaintext("sts_endpoint").asKeylessAuth();
    private static final DataSourceConfigDefinition STS_REGION = plaintext("sts_region").asKeylessAuth();

    private static final Map<String, DataSourceConfigDefinition> FIELDS = DataSourceConfigDefinition.mapOf(
        ACCESS_KEY,
        SECRET_KEY,
        SESSION_TOKEN,
        ENDPOINT,
        REGION,
        ROLE_ARN,
        ROLE_SESSION_NAME,
        JWT_AUDIENCE,
        STS_ENDPOINT,
        STS_REGION,
        AUTH
    );

    private S3Configuration(Map<String, Object> raw) {
        super(raw, FIELDS);
    }

    @Override
    protected void validateCredentials(ValidationException errors) {
        // role_session_name and sts_endpoint are optional; role_arn and jwt_audience are the minimum
        // needed to mint an OIDC token and exchange it for credentials via STS AssumeRoleWithWebIdentity.
        if (hasKeylessAuth()) {
            if (roleArn() == null) {
                errors.addValidationError("role_arn is required when keyless authentication settings are configured");
            }
            if (jwtAudience() == null) {
                errors.addValidationError("jwt_audience is required when keyless authentication settings are configured");
            }
        }
    }

    public static S3Configuration fromMap(Map<String, Object> raw) {
        return raw == null || raw.isEmpty() ? null : new S3Configuration(raw);
    }

    /**
     * Lenient factory for query-time configuration maps, which may carry format-level options
     * (e.g. {@code header_row}) alongside storage-level options. Filters unknown keys
     * before construction; cross-field validation (auth/credential conflicts) still runs.
     */
    public static Configured<S3Configuration> fromQueryConfig(Map<String, Object> raw) {
        return filterAndConstruct(raw, FIELDS, S3Configuration::new);
    }

    public static S3Configuration fromFields(String accessKey, String secretKey, String endpoint, String region) {
        return fromFields(accessKey, secretKey, endpoint, region, null);
    }

    public static S3Configuration fromFields(String accessKey, String secretKey, String endpoint, String region, String auth) {
        return fromFields(accessKey, secretKey, null, endpoint, region, auth);
    }

    public static S3Configuration fromFields(
        String accessKey,
        String secretKey,
        String sessionToken,
        String endpoint,
        String region,
        String auth
    ) {
        var raw = buildRawMap(
            ACCESS_KEY,
            accessKey,
            SECRET_KEY,
            secretKey,
            SESSION_TOKEN,
            sessionToken,
            ENDPOINT,
            endpoint,
            REGION,
            region,
            AUTH,
            auth
        );
        return raw != null ? fromMap(raw) : null;
    }

    /**
     * Builds a keyless workload-identity configuration. {@code roleSessionName}, {@code stsEndpoint}, and
     * {@code stsRegion} are optional.
     */
    public static S3Configuration fromKeylessFields(
        String roleArn,
        String roleSessionName,
        String jwtAudience,
        String stsEndpoint,
        String stsRegion,
        String endpoint,
        String region
    ) {
        var raw = buildRawMap(
            ROLE_ARN,
            roleArn,
            ROLE_SESSION_NAME,
            roleSessionName,
            JWT_AUDIENCE,
            jwtAudience,
            STS_ENDPOINT,
            stsEndpoint,
            STS_REGION,
            stsRegion,
            ENDPOINT,
            endpoint,
            REGION,
            region
        );
        return raw != null ? fromMap(raw) : null;
    }

    public String accessKey() {
        return get(ACCESS_KEY.name());
    }

    public String secretKey() {
        return get(SECRET_KEY.name());
    }

    public String sessionToken() {
        return get(SESSION_TOKEN.name());
    }

    public String endpoint() {
        return get(ENDPOINT.name());
    }

    public String region() {
        return get(REGION.name());
    }

    /** The IAM role ARN to assume via STS {@code AssumeRoleWithWebIdentity} on the keyless path. */
    public String roleArn() {
        return get(ROLE_ARN.name());
    }

    /** Optional session name for the assumed-role session; a default is used when absent. */
    public String roleSessionName() {
        return get(ROLE_SESSION_NAME.name());
    }

    /** Audience passed to the workload-identity issuer when minting the OIDC token presented to STS. */
    public String jwtAudience() {
        return get(JWT_AUDIENCE.name());
    }

    /** Optional STS endpoint override (e.g. a regional endpoint or test fixture); defaults to the SDK resolution. */
    public String stsEndpoint() {
        return get(STS_ENDPOINT.name());
    }

    /**
     * Optional region for the STS client, independent of the bucket {@link #region()}. STS uses regional endpoints
     * ({@code sts.<region>.amazonaws.com}), so this allows assuming the role through a different region than the
     * bucket. When unset, the bucket region is used (which also keeps STS in the bucket's AWS partition).
     */
    public String stsRegion() {
        return get(STS_REGION.name());
    }

    public boolean hasCredentials() {
        return accessKey() != null && secretKey() != null;
    }
}
