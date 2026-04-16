/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.ValidationException;

import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.plaintext;

/**
 * Base configuration for file-based external sources (S3, GCS, Azure). Handles common
 * authentication: the {@code auth} field, anonymous access detection, and validation
 * that auth=none is not combined with explicit credentials. Credential conflict detection
 * is automatic — any field marked {@link DataSourceConfigDefinition#secret(String) secret}
 * that has a value set is treated as an explicit credential.
 */
public abstract class FileDataSourceConfiguration extends DataSourceConfiguration {

    protected static final DataSourceConfigDefinition AUTH = plaintext("auth").asCaseInsensitive();
    private static final String AUTH_NONE = "none";

    protected FileDataSourceConfiguration(Map<String, Object> raw, Map<String, DataSourceConfigDefinition> fieldDefs) {
        super(raw, fieldDefs);
    }

    @Override
    protected final void validate(ValidationException errors) {
        if (auth() != null && AUTH_NONE.equals(auth()) == false) {
            errors.addValidationError("Unsupported auth value [" + auth() + "]; supported values: [none]");
        }
        if (isAnonymous() && hasAnySecretValue()) {
            errors.addValidationError("auth=none cannot be combined with explicit credentials; anonymous access uses no credentials");
        }
    }

    public String auth() {
        return get(AUTH.name());
    }

    public boolean isAnonymous() {
        return AUTH_NONE.equals(auth());
    }
}
