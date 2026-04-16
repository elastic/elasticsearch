/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.s3;

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
 *   <li>{@code auth=none} for anonymous access to public buckets</li>
 *   <li>Default credentials (IAM role, instance profile) when no explicit credentials are provided</li>
 * </ul>
 */
public class S3Configuration extends FileDataSourceConfiguration {

    private static final DataSourceConfigDefinition ACCESS_KEY = secret("access_key");
    private static final DataSourceConfigDefinition SECRET_KEY = secret("secret_key");
    private static final DataSourceConfigDefinition ENDPOINT = plaintext("endpoint");
    private static final DataSourceConfigDefinition REGION = plaintext("region");

    private static final Map<String, DataSourceConfigDefinition> FIELDS = DataSourceConfigDefinition.mapOf(
        ACCESS_KEY,
        SECRET_KEY,
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

    public static S3Configuration fromFields(String accessKey, String secretKey, String endpoint, String region) {
        return fromFields(accessKey, secretKey, endpoint, region, null);
    }

    public static S3Configuration fromFields(String accessKey, String secretKey, String endpoint, String region, String auth) {
        var raw = buildRawMap(ACCESS_KEY, accessKey, SECRET_KEY, secretKey, ENDPOINT, endpoint, REGION, region, AUTH, auth);
        return raw != null ? fromMap(raw) : null;
    }

    public String accessKey() {
        return get(ACCESS_KEY.name());
    }

    public String secretKey() {
        return get(SECRET_KEY.name());
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
