/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.xpack.esql.datasources.spi.ConfigSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DatasourceConfiguration;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

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
public class S3Configuration extends DatasourceConfiguration {

    private static final Map<String, ConfigSetting> SETTINGS = ConfigSetting.mapOf(
        new ConfigSetting("access_key", true),
        new ConfigSetting("secret_key", true),
        new ConfigSetting("endpoint", false),
        new ConfigSetting("region", false),
        new ConfigSetting("auth", false)
    );

    private S3Configuration(Map<String, Object> raw) {
        super(raw, SETTINGS);
    }

    @Override
    protected String normalizeValue(String key, String value) {
        if ("auth".equals(key)) {
            return value.toLowerCase(Locale.ROOT);
        }
        return value;
    }

    @Override
    protected void validate() {
        if (auth() != null && "none".equals(auth()) == false) {
            throw new IllegalArgumentException("Unsupported auth value [" + auth() + "]; supported values: [none]");
        }
        if (isAnonymous() && (accessKey() != null || secretKey() != null)) {
            throw new IllegalArgumentException(
                "auth=none cannot be combined with access_key/secret_key; anonymous access uses no credentials"
            );
        }
    }

    public static S3Configuration fromMap(Map<String, Object> raw) {
        return raw == null || raw.isEmpty() ? null : new S3Configuration(raw);
    }

    public static S3Configuration fromFields(String accessKey, String secretKey, String endpoint, String region) {
        return fromFields(accessKey, secretKey, endpoint, region, null);
    }

    public static S3Configuration fromFields(String accessKey, String secretKey, String endpoint, String region, String auth) {
        Map<String, Object> raw = new HashMap<>();
        if (accessKey != null) raw.put("access_key", accessKey);
        if (secretKey != null) raw.put("secret_key", secretKey);
        if (endpoint != null) raw.put("endpoint", endpoint);
        if (region != null) raw.put("region", region);
        if (auth != null) raw.put("auth", auth);
        return raw.isEmpty() ? null : fromMap(raw);
    }

    public String accessKey() {
        return get("access_key");
    }

    public String secretKey() {
        return get("secret_key");
    }

    public String endpoint() {
        return get("endpoint");
    }

    public String region() {
        return get("region");
    }

    public String auth() {
        return get("auth");
    }

    public boolean isAnonymous() {
        return "none".equals(auth());
    }

    public boolean hasCredentials() {
        return accessKey() != null && secretKey() != null;
    }
}
