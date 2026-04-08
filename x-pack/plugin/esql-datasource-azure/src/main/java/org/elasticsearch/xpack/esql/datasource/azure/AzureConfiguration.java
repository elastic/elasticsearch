/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.datasources.spi.DatasourceConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Azure Blob Storage access including credentials and endpoint settings.
 */
public class AzureConfiguration extends DatasourceConfiguration {

    public static final Map<String, Boolean> FIELDS = Map.ofEntries(
        Map.entry("connection_string", true),
        Map.entry("account", false),
        Map.entry("key", true),
        Map.entry("sas_token", true),
        Map.entry("endpoint", false),
        Map.entry("auth", false)
    );

    private AzureConfiguration(Map<String, String> settings) {
        super(settings);
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
        if ("none".equals(auth()) && hasExplicitCredentials()) {
            throw new IllegalArgumentException(
                "auth=none cannot be combined with connection_string/account+key/sas_token; anonymous access uses no credentials"
            );
        }
    }

    public static AzureConfiguration fromMap(Map<String, Object> raw) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        return new AzureConfiguration(parseRaw(raw, FIELDS));
    }

    public static AzureConfiguration fromParams(Map<String, Expression> params) {
        if (params == null || params.isEmpty()) {
            return null;
        }
        Map<String, Object> raw = new HashMap<>();
        for (String field : FIELDS.keySet()) {
            String value = extractStringParam(params, field);
            if (value != null) {
                raw.put(field, value);
            }
        }
        return raw.isEmpty() ? null : fromMap(raw);
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
        Map<String, Object> raw = new HashMap<>();
        if (connectionString != null) raw.put("connection_string", connectionString);
        if (account != null) raw.put("account", account);
        if (key != null) raw.put("key", key);
        if (sasToken != null) raw.put("sas_token", sasToken);
        if (endpoint != null) raw.put("endpoint", endpoint);
        if (auth != null) raw.put("auth", auth);
        return raw.isEmpty() ? null : fromMap(raw);
    }

    private static String extractStringParam(Map<String, Expression> params, String key) {
        Expression expr = params.get(key);
        if (expr instanceof Literal literal) {
            Object value = literal.value();
            if (value instanceof BytesRef bytesRef) {
                return BytesRefs.toString(bytesRef);
            }
            return value != null ? value.toString() : null;
        }
        return null;
    }

    public String connectionString() {
        return get("connection_string");
    }

    public String account() {
        return get("account");
    }

    public String key() {
        return get("key");
    }

    public String sasToken() {
        return get("sas_token");
    }

    public String endpoint() {
        return get("endpoint");
    }

    public String auth() {
        return get("auth");
    }

    public boolean isAnonymous() {
        return "none".equals(auth());
    }

    public boolean hasCredentials() {
        return hasExplicitCredentials();
    }

    private boolean hasExplicitCredentials() {
        return (connectionString() != null && connectionString().isEmpty() == false)
            || (account() != null && key() != null)
            || (sasToken() != null && sasToken().isEmpty() == false);
    }
}
