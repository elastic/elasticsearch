/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfiguration;

import java.util.Map;

/**
 * Minimal {@link DataSourceConfiguration} for unauthenticated sources (HTTP/HTTPS and local files).
 *
 * <p>It declares a single optional field, {@code auth}, whose only accepted value is {@code none} —
 * letting a definition explicitly state "anonymous access" symmetrically with the file-based sources
 * (S3/GCS/Azure), even though that is already the default. Every other datasource-level setting is
 * rejected as an unknown field by the base-class constructor: these sources carry neither credentials
 * nor tunable storage options today. The matching storage providers in {@link HttpDataSourcePlugin}
 * are registered with {@code StorageProviderFactory.noConfigKeys(...)}, so there is nothing to thread
 * from the stored datasource into the read path. Should authentication or per-source knobs (timeouts,
 * headers) be added later, this is where their field definitions would live.
 */
public final class NoAuthDataSourceConfiguration extends DataSourceConfiguration {

    private static final DataSourceConfigDefinition AUTH = DataSourceConfigDefinition.plaintext("auth").asCaseInsensitive();
    private static final String AUTH_NONE = "none";
    private static final Map<String, DataSourceConfigDefinition> FIELDS = DataSourceConfigDefinition.mapOf(AUTH);

    private NoAuthDataSourceConfiguration(Map<String, Object> raw) {
        super(raw, FIELDS);
    }

    @Override
    protected void validate(ValidationException errors) {
        String auth = get(AUTH.name());
        if (auth != null && AUTH_NONE.equals(auth) == false) {
            errors.addValidationError("Unsupported auth value [" + auth + "]; the only supported value is [none]");
        }
    }

    /** Returns {@code null} for empty input so callers treat a settings-less datasource as "no configuration". */
    public static NoAuthDataSourceConfiguration fromMap(Map<String, Object> raw) {
        return raw == null || raw.isEmpty() ? null : new NoAuthDataSourceConfiguration(raw);
    }
}
