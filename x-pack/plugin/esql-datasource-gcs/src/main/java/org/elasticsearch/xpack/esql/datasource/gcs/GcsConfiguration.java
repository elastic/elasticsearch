/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceConfiguration;

import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.plaintext;
import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.secret;

/**
 * Configuration for Google Cloud Storage access including credentials and endpoint settings.
 * <p>
 * Supports authentication modes:
 * <ul>
 *   <li>Service account JSON credentials (inline)</li>
 *   <li>{@code auth=none} for anonymous access to public buckets</li>
 *   <li>Application Default Credentials (ADC) when no explicit credentials are provided</li>
 * </ul>
 */
public class GcsConfiguration extends FileDataSourceConfiguration {

    private static final DataSourceConfigDefinition CREDENTIALS = secret("credentials");
    private static final DataSourceConfigDefinition PROJECT_ID = plaintext("project_id");
    private static final DataSourceConfigDefinition ENDPOINT = plaintext("endpoint");
    private static final DataSourceConfigDefinition TOKEN_URI = plaintext("token_uri");

    private static final Map<String, DataSourceConfigDefinition> FIELDS = DataSourceConfigDefinition.mapOf(
        CREDENTIALS,
        PROJECT_ID,
        ENDPOINT,
        TOKEN_URI,
        AUTH
    );

    private GcsConfiguration(Map<String, Object> raw) {
        super(raw, FIELDS);
    }

    public static GcsConfiguration fromMap(Map<String, Object> raw) {
        return raw == null || raw.isEmpty() ? null : new GcsConfiguration(raw);
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
        var raw = buildRawMap(
            CREDENTIALS,
            serviceAccountCredentials,
            PROJECT_ID,
            projectId,
            ENDPOINT,
            endpoint,
            TOKEN_URI,
            tokenUri,
            AUTH,
            auth
        );
        return raw != null ? fromMap(raw) : null;
    }

    public String serviceAccountCredentials() {
        return get(CREDENTIALS.name());
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

    public boolean hasCredentials() {
        return serviceAccountCredentials() != null;
    }
}
