/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfiguration;

import java.net.URI;
import java.util.Map;
import java.util.Set;

/** Keeps connection settings separate from the database and table selected by a dataset. */
final class ClickHouseConfiguration extends DataSourceConfiguration {
    static final Map<String, DataSourceConfigDefinition> FIELDS = DataSourceConfigDefinition.mapOf(
        DataSourceConfigDefinition.plaintext("endpoint"),
        DataSourceConfigDefinition.plaintext("username"),
        DataSourceConfigDefinition.secret("password"),
        DataSourceConfigDefinition.plaintext("auth")
    );

    private ClickHouseConfiguration(Map<String, Object> settings, Set<String> existingSecrets) {
        super(settings, FIELDS, existingSecrets);
    }

    static ClickHouseConfiguration fromMap(Map<String, Object> settings, Set<String> existingSecrets) {
        return new ClickHouseConfiguration(settings, existingSecrets);
    }

    @Override
    protected void validate(ValidationException errors) {
        for (var entry : toMap().entrySet()) {
            if (entry.getValue() instanceof CharSequence == false) {
                errors.addValidationError("ClickHouse setting [" + entry.getKey() + "] must be a string");
            } else if (entry.getValue().toString().length() > 4096) {
                errors.addValidationError("ClickHouse setting [" + entry.getKey() + "] exceeds 4096 characters");
            }
        }
        if (get("auth") != null && "basic".equals(get("auth")) == false) {
            errors.addValidationError("ClickHouse supports [basic] authentication");
        }
        String username = get("username");
        if (username == null || username.isBlank() || username.contains(":") || username.contains("\n") || username.contains("\r")) {
            errors.addValidationError("ClickHouse requires a username without colons or line breaks");
        }
        if (hasStoredSecret("password") == false) {
            errors.addValidationError("ClickHouse requires a password");
        }
        try {
            endpoint(get("endpoint"));
        } catch (IllegalArgumentException e) {
            errors.addValidationError(e.getMessage());
        }
    }

    static URI endpoint(String value) {
        try {
            URI uri = URI.create(value == null ? "" : value);
            boolean localHttp = "http".equals(uri.getScheme())
                && Set.of("localhost", "127.0.0.1", "[::1]").contains(uri.getHost() == null ? "" : uri.getHost());
            if (("https".equals(uri.getScheme()) || localHttp)
                && uri.getHost() != null
                && uri.getUserInfo() == null
                && uri.getQuery() == null
                && uri.getFragment() == null
                && (uri.getPath().isEmpty() || uri.getPath().equals("/"))) {
                return uri;
            }
        } catch (IllegalArgumentException e) {
            // Do not expose a malformed URL that could contain credentials.
        }
        throw new IllegalArgumentException("ClickHouse endpoint must be an HTTPS origin; HTTP is allowed only on loopback");
    }
}
