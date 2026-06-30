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
 * that auth=none is not combined with explicit credentials or keyless authentication
 * settings. Credential conflict detection is automatic — any field marked
 * {@link DataSourceConfigDefinition#secret(String) secret} or
 * {@link DataSourceConfigDefinition#asKeylessAuth() keyless auth} that has a value set is
 * treated as an authentication setting, and the two kinds cannot be combined.
 */
public abstract class FileDataSourceConfiguration extends DataSourceConfiguration {

    protected static final DataSourceConfigDefinition AUTH = plaintext("auth").asCaseInsensitive();
    private static final String AUTH_NONE = "none";
    private static final String AUTH_WORKLOAD_IDENTITY = "workload_identity";

    /**
     * Error shown when {@code auth=workload_identity} is requested while the cluster setting is disabled. Shared between
     * the CRUD validator ({@link FileDataSourceValidator#validateDatasource}) and the inline-WITH gate in
     * {@code StorageProviderRegistry} so both paths report the same message.
     */
    public static final String WORKLOAD_IDENTITY_DISABLED_MESSAGE =
        "auth=workload_identity requires the [esql.datasource.workload_identity.enabled] cluster setting to be enabled; "
            + "it is disabled by default — enable it only on single-cloud single-tenant deployments";

    protected FileDataSourceConfiguration(Map<String, Object> raw, Map<String, DataSourceConfigDefinition> fieldDefs) {
        super(raw, fieldDefs);
    }

    @Override
    protected final void validate(ValidationException errors) {
        if (auth() != null && AUTH_NONE.equals(auth()) == false && AUTH_WORKLOAD_IDENTITY.equals(auth()) == false) {
            errors.addValidationError("Unsupported auth value [" + auth() + "]; supported values: [none, workload_identity]");
        }
        if (isAnonymous() && hasAnySecretValue()) {
            errors.addValidationError("auth=none cannot be combined with explicit credentials; anonymous access uses no credentials");
        }
        if (isAnonymous() && hasKeylessAuth()) {
            errors.addValidationError(
                "auth=none cannot be combined with keyless authentication settings; anonymous access uses no credentials"
            );
        }
        if (isWorkloadIdentity() && hasAnySecretValue()) {
            errors.addValidationError("auth=workload_identity cannot be combined with explicit credentials");
        }
        if (isWorkloadIdentity() && hasKeylessAuth()) {
            errors.addValidationError("auth=workload_identity cannot be combined with keyless authentication settings");
        }
        if (hasAnySecretValue() && hasKeylessAuth()) {
            errors.addValidationError("explicit credentials cannot be combined with keyless authentication settings");
        }
        validateCredentials(errors);
    }

    /** Subclass hook for datasource-specific credential validation. */
    protected void validateCredentials(ValidationException errors) {}

    public String auth() {
        return get(AUTH.name());
    }

    public boolean isAnonymous() {
        return AUTH_NONE.equals(auth());
    }

    /** Returns true when {@code auth=workload_identity} is set, directing the provider to use the node's instance credentials. */
    public boolean isWorkloadIdentity() {
        return isWorkloadIdentityAuth(auth());
    }

    /**
     * Returns true when the raw {@code auth} value selects workload-identity. Shared so the inline-WITH gate in
     * {@code StorageProviderRegistry} (which inspects an unparsed config map) and {@link #isWorkloadIdentity()}
     * agree on what counts as {@code auth=workload_identity} instead of each re-implementing the comparison.
     */
    public static boolean isWorkloadIdentityAuth(Object authValue) {
        return authValue instanceof String s && AUTH_WORKLOAD_IDENTITY.equalsIgnoreCase(s);
    }
}
