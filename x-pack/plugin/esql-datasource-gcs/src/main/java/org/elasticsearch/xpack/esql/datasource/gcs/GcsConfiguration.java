/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.xpack.esql.datasources.spi.DatasourceConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Google Cloud Storage access including credentials and endpoint settings.
 */
public class GcsConfiguration extends DatasourceConfiguration {

    private static final Map<String, Boolean> FIELDS = Map.ofEntries(
        Map.entry("credentials", true),
        Map.entry("project_id", false),
        Map.entry("endpoint", false),
        Map.entry("token_uri", false),
        Map.entry("auth", false)
    );

    private GcsConfiguration(Map<String, Object> raw) {
        super(raw);
    }

    @Override
    public Map<String, Boolean> fields() {
        return FIELDS;
    }

    @Override
    protected void validate() {
        if (auth() != null && "none".equals(auth()) == false) {
            throw new IllegalArgumentException("Unsupported auth value [" + auth() + "]; supported values: [none]");
        }
        if ("none".equals(auth()) && serviceAccountCredentials() != null) {
            throw new IllegalArgumentException("auth=none cannot be combined with credentials; anonymous access uses no credentials");
        }
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
        Map<String, Object> raw = new HashMap<>();
        if (serviceAccountCredentials != null) raw.put("credentials", serviceAccountCredentials);
        if (projectId != null) raw.put("project_id", projectId);
        if (endpoint != null) raw.put("endpoint", endpoint);
        if (tokenUri != null) raw.put("token_uri", tokenUri);
        if (auth != null) raw.put("auth", auth);
        return raw.isEmpty() ? null : fromMap(raw);
    }

    public String serviceAccountCredentials() {
        return get("credentials");
    }

    public String projectId() {
        return get("project_id");
    }

    public String endpoint() {
        return get("endpoint");
    }

    public String tokenUri() {
        return get("token_uri");
    }

    public String auth() {
        return get("auth");
    }

    public boolean isAnonymous() {
        return "none".equals(auth());
    }

    public boolean hasCredentials() {
        return serviceAccountCredentials() != null;
    }
}
