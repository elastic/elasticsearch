/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.plaintext;

/**
 * Base configuration for file-based external sources (S3, GCS, Azure). Handles the {@code auth}
 * mode and the cross-field rules that bind it to the credential fields.
 *
 * <p>{@code auth} is an explicit mode switch with these canonical values:
 * <ul>
 *   <li>{@link #AUTH_AUTO auto} (the default when {@code auth} is omitted) — resolve the mode from
 *       the fields present: keyless settings → {@code federated_identity}, a stored secret →
 *       {@code static_credentials} (the {@code auth} value itself stays {@code auto} and is not rewritten).
 *       A credential-less {@code auto} config parses and is rejected later, at credential-resolution time, by
 *       the storage provider (matching the pre-existing behavior of an omitted {@code auth}). Never resolves to
 *       {@code anonymous} or {@code managed_identity} — those are opt-in only.</li>
 *   <li>{@link #AUTH_ANONYMOUS anonymous} — public / unauthenticated access (no credentials).</li>
 *   <li>{@link #AUTH_STATIC_CREDENTIALS static_credentials} — a stored long-lived secret.</li>
 *   <li>{@link #AUTH_FEDERATED_IDENTITY federated_identity} — issuer-minted OIDC federation
 *       (keyless settings such as a role ARN + audience).</li>
 *   <li>{@link #AUTH_MANAGED_IDENTITY managed_identity} — the node's ambient cloud identity
 *       (instance profile / IMDS / metadata server); gated by a cluster setting.</li>
 * </ul>
 *
 * <p>Backwards compatibility: the former value names {@code none} and {@code workload_identity}
 * are still accepted, canonicalized to {@code anonymous} and {@code managed_identity} on parse (so a
 * stored configuration and every later read hold the canonical value), and each use emits a
 * deprecation warning. See {@link #normalize(Map)} and {@link #DEPRECATED_AUTH_ALIASES}.
 *
 * <p>Credential-conflict detection is automatic — any field marked
 * {@link DataSourceConfigDefinition#secret(String) secret} or
 * {@link DataSourceConfigDefinition#asKeylessAuth() keyless auth} that has a value set is treated as
 * an authentication setting, and the two kinds cannot be combined.
 */
public abstract class FileDataSourceConfiguration extends DataSourceConfiguration {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(FileDataSourceConfiguration.class);

    protected static final DataSourceConfigDefinition AUTH = plaintext("auth").asCaseInsensitive();

    /** Resolve the mode from the fields present (the default when {@code auth} is omitted). */
    static final String AUTH_AUTO = "auto";
    /** Public / unauthenticated access. */
    static final String AUTH_ANONYMOUS = "anonymous";
    /** A stored long-lived secret (access key + secret, service-account JSON, account key, SAS, ...). */
    static final String AUTH_STATIC_CREDENTIALS = "static_credentials";
    /** Issuer-minted OIDC federation, configured via keyless settings. */
    static final String AUTH_FEDERATED_IDENTITY = "federated_identity";
    /** The node's ambient cloud identity (instance profile / IMDS / metadata server). */
    static final String AUTH_MANAGED_IDENTITY = "managed_identity";

    /**
     * Deprecated {@code auth} value names, mapped to their canonical replacement. Still accepted for
     * backwards compatibility; {@link #normalize(Map)} rewrites them to the canonical value on parse and
     * emits a deprecation warning per use.
     */
    private static final Map<String, String> DEPRECATED_AUTH_ALIASES = Map.of(
        "none",
        AUTH_ANONYMOUS,
        "workload_identity",
        AUTH_MANAGED_IDENTITY
    );

    /**
     * Canonical {@code auth} values, in display order. The single source for both the accepted-value
     * check ({@link #SUPPORTED_AUTH}) and the unsupported-value error message, so the two cannot drift.
     */
    private static final List<String> CANONICAL_AUTH_VALUES = List.of(
        AUTH_AUTO,
        AUTH_ANONYMOUS,
        AUTH_STATIC_CREDENTIALS,
        AUTH_FEDERATED_IDENTITY,
        AUTH_MANAGED_IDENTITY
    );

    private static final Set<String> SUPPORTED_AUTH = Set.copyOf(CANONICAL_AUTH_VALUES);

    /**
     * Error shown when {@code auth=managed_identity} is requested while the cluster setting is disabled. Shared between
     * the CRUD validator ({@link FileDataSourceValidator#validateDatasource}) and the inline-WITH gate in
     * {@code StorageProviderRegistry} so both paths report the same message.
     */
    public static final String MANAGED_IDENTITY_DISABLED_MESSAGE =
        "auth=managed_identity requires the [esql.datasource.managed_identity.enabled] cluster setting to be enabled; "
            + "it is disabled by default — enable it only on single-cloud single-tenant deployments";

    protected FileDataSourceConfiguration(Map<String, Object> raw, Map<String, DataSourceConfigDefinition> fieldDefs) {
        super(raw, fieldDefs);
    }

    @Override
    protected void normalize(Map<String, Object> parsed) {
        Object value = parsed.get(AUTH.name());
        if (value instanceof String s) {
            String canonical = DEPRECATED_AUTH_ALIASES.get(s);
            if (canonical != null) {
                deprecationLogger.warn(
                    DeprecationCategory.API,
                    "esql_datasource_auth_" + s,
                    "auth value [{}] is deprecated; the canonical value is [{}]",
                    s,
                    canonical
                );
                parsed.put(AUTH.name(), canonical);
            }
        }
    }

    @Override
    protected final void validate(ValidationException errors) {
        String auth = auth();
        if (auth != null && SUPPORTED_AUTH.contains(auth) == false) {
            errors.addValidationError(
                "Unsupported auth value [" + auth + "]; supported values: [" + String.join(", ", CANONICAL_AUTH_VALUES) + "]"
            );
        }
        boolean secrets = hasAnySecretValue();
        boolean keyless = hasKeylessAuth();
        if (isAnonymous()) {
            if (secrets) {
                errors.addValidationError(
                    "auth=anonymous cannot be combined with explicit credentials; anonymous access uses no credentials"
                );
            }
            if (keyless) {
                errors.addValidationError(
                    "auth=anonymous cannot be combined with keyless authentication settings; anonymous access uses no credentials"
                );
            }
        }
        if (isManagedIdentity()) {
            if (secrets) {
                errors.addValidationError("auth=managed_identity cannot be combined with explicit credentials");
            }
            if (keyless) {
                errors.addValidationError("auth=managed_identity cannot be combined with keyless authentication settings");
            }
        }
        if (isStaticCredentials()) {
            // A complete static credential is required — a partial secret set (e.g. an S3 session_token with no
            // access/secret key) would otherwise pass here and only fail at provider-build time. Mirrors the
            // per-mode completeness that federated_identity gets via validateCredentials().
            if (hasCredentials() == false) {
                errors.addValidationError("auth=static_credentials requires complete explicit credentials");
            }
            if (keyless) {
                errors.addValidationError("auth=static_credentials cannot be combined with keyless authentication settings");
            }
        }
        if (isFederatedIdentity()) {
            if (keyless == false) {
                errors.addValidationError("auth=federated_identity requires keyless authentication settings");
            }
            if (secrets) {
                errors.addValidationError("auth=federated_identity cannot be combined with explicit credentials");
            }
        }
        if (secrets && keyless) {
            errors.addValidationError("explicit credentials cannot be combined with keyless authentication settings");
        }
        validateCredentials(errors);
    }

    /** Subclass hook for datasource-specific credential validation. */
    protected void validateCredentials(ValidationException errors) {}

    /**
     * Returns true when the configuration carries a <em>complete</em> static credential. The exact required field
     * combination is CSP-specific (e.g. S3 access_key+secret_key; Azure connection_string or account+key/sas_token).
     * Used by {@link #validate} to enforce {@code auth=static_credentials} completeness and by the providers to
     * select the static-credential path.
     */
    public abstract boolean hasCredentials();

    public String auth() {
        return get(AUTH.name());
    }

    public boolean isAnonymous() {
        return AUTH_ANONYMOUS.equals(auth());
    }

    /** Returns true when {@code auth=static_credentials} is explicitly set. */
    public boolean isStaticCredentials() {
        return AUTH_STATIC_CREDENTIALS.equals(auth());
    }

    /** Returns true when {@code auth=federated_identity} is explicitly set. */
    public boolean isFederatedIdentity() {
        return AUTH_FEDERATED_IDENTITY.equals(auth());
    }

    /** Returns true when {@code auth=managed_identity} is set, directing the provider to use the node's ambient credentials. */
    public boolean isManagedIdentity() {
        return isManagedIdentityAuth(auth());
    }

    /**
     * Returns true when the raw {@code auth} value selects managed-identity. Shared so the inline-WITH gate in
     * {@code StorageProviderRegistry} (which inspects an unparsed config map) and {@link #isManagedIdentity()}
     * agree on what counts as {@code auth=managed_identity} instead of each re-implementing the comparison. Accepts
     * the deprecated {@code workload_identity} alias too, because the inline-WITH gate sees the raw value before it
     * has been canonicalized.
     */
    public static boolean isManagedIdentityAuth(Object authValue) {
        if (authValue instanceof String s) {
            String lower = s.toLowerCase(Locale.ROOT);
            // Match the canonical value or any deprecated alias that canonicalizes to it — keeping the alias
            // vocabulary in one place ({@link #DEPRECATED_AUTH_ALIASES}) rather than re-listing literals here.
            return AUTH_MANAGED_IDENTITY.equals(lower) || AUTH_MANAGED_IDENTITY.equals(DEPRECATED_AUTH_ALIASES.get(lower));
        }
        return false;
    }
}
