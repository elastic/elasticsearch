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
import java.util.Locale;
import java.util.Map;

/**
 * Configuration for Azure Blob Storage access including credentials and endpoint settings.
 * <p>
 * Supports authentication modes:
 * <ul>
 *   <li>Connection string (full connection string)</li>
 *   <li>Account + key (SharedKey auth)</li>
 *   <li>SAS token</li>
 *   <li>{@code auth=none} for anonymous access to public containers</li>
 *   <li>DefaultAzureCredential when no explicit credentials are provided</li>
 * </ul>
 */
public class AzureConfiguration extends DatasourceConfiguration {

    private static final ConfigSetting CONNECTION_STRING = ConfigSetting.secret("connection_string");
    private static final ConfigSetting ACCOUNT = ConfigSetting.plaintext("account");
    private static final ConfigSetting KEY = ConfigSetting.secret("key");
    private static final ConfigSetting SAS_TOKEN = ConfigSetting.secret("sas_token");
    private static final ConfigSetting ENDPOINT = ConfigSetting.plaintext("endpoint");
    private static final ConfigSetting AUTH = ConfigSetting.plaintext("auth");

    private static final Map<String, ConfigSetting> SETTINGS = ConfigSetting.mapOf(
        CONNECTION_STRING,
        ACCOUNT,
        KEY,
        SAS_TOKEN,
        ENDPOINT,
        AUTH
    );

    private AzureConfiguration(Map<String, Object> raw) {
        super(raw, SETTINGS);
    }

    @Override
    protected String normalizeValue(String key, String value) {
        if (AUTH.name().equals(key)) {
            return value.toLowerCase(Locale.ROOT);
        }
        return value;
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
        if (connectionString != null) raw.put(CONNECTION_STRING.name(), connectionString);
        if (account != null) raw.put(ACCOUNT.name(), account);
        if (key != null) raw.put(KEY.name(), key);
        if (sasToken != null) raw.put(SAS_TOKEN.name(), sasToken);
        if (endpoint != null) raw.put(ENDPOINT.name(), endpoint);
        if (auth != null) raw.put(AUTH.name(), auth);
        return raw.isEmpty() ? null : fromMap(raw);
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

    public String auth() {
        return get(AUTH.name());
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
