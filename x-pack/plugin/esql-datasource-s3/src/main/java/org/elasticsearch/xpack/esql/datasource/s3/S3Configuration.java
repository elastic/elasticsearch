/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.s3;

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
 *   <li>{@code auth=none} for anonymous access to public buckets</li>
 *   <li>{@code auth=ambient} to use the node's instance credential chain (env vars, EKS IRSA, EC2 instance profile).
 *       Requires the {@code esql.datasource.ambient_credentials.enabled} cluster setting.</li>
 * </ul>
 */
public class S3Configuration extends FileDataSourceConfiguration {

    private static final DataSourceConfigDefinition ACCESS_KEY = secret("access_key");
    private static final DataSourceConfigDefinition SECRET_KEY = secret("secret_key");
    private static final DataSourceConfigDefinition SESSION_TOKEN = secret("session_token");
    private static final DataSourceConfigDefinition ENDPOINT = plaintext("endpoint");
    private static final DataSourceConfigDefinition REGION = plaintext("region");

    private static final Map<String, DataSourceConfigDefinition> FIELDS = DataSourceConfigDefinition.mapOf(
        ACCESS_KEY,
        SECRET_KEY,
        SESSION_TOKEN,
        ENDPOINT,
        REGION,
        AUTH
    );

    private S3Configuration(Map<String, Object> raw) {
        super(raw, FIELDS);
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

    public boolean hasCredentials() {
        return accessKey() != null && secretKey() != null;
    }
}
