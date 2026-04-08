/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.xpack.esql.datasources.spi.ConfigSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DatasourceConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Azure Blob Storage access including credentials and endpoint settings.
 */
public class AzureConfiguration extends DatasourceConfiguration {

    private static final Map<String, ConfigSetting> SETTINGS = ConfigSetting.mapOf(
        new ConfigSetting("connection_string", true),
        new ConfigSetting("account", false),
        new ConfigSetting("key", true),
        new ConfigSetting("sas_token", true),
        new ConfigSetting("endpoint", false),
        new ConfigSetting("auth", false)
    );

    private AzureConfiguration(Map<String, Object> raw) {
        super(raw);
    }

    @Override
    public Map<String, ConfigSetting> settings() {
        return SETTINGS;
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
        return raw == null || raw.isEmpty() ? null : new AzureConfiguration(raw);
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
