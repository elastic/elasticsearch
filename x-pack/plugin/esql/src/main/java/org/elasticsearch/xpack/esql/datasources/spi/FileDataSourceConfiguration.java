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
 *       the fields present: federated authentication settings → {@code federated_identity}, a stored secret →
 *       {@code static_credentials} (the {@code auth} value itself stays {@code auto} and is not rewritten).
 *       A credential-less {@code auto} config is unresolvable and is rejected at create time by
 *       {@link #validate} — not deferred to the storage provider. Never resolves to {@code anonymous} or
 *       {@code managed_identity} — those are opt-in only.</li>
 *   <li>{@link #AUTH_ANONYMOUS anonymous} — public / unauthenticated access (no credentials).</li>
 *   <li>{@link #AUTH_STATIC_CREDENTIALS static_credentials} — a stored long-lived secret.</li>
 *   <li>{@link #AUTH_FEDERATED_IDENTITY federated_identity} — issuer-minted OIDC federation
 *       (federated settings such as a role ARN + audience).</li>
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
 * {@link DataSourceConfigDefinition#asFederatedAuth() federated auth} that has a value set is treated as
 * an authentication setting, and the two kinds cannot be combined.
 */
public abstract class FileDataSourceConfiguration extends DataSourceConfiguration {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(FileDataSourceConfiguration.class);

    protected static final DataSourceConfigDefinition AUTH = plaintext("auth").asCaseInsensitive();

    /** Resolve the mode from the fields present (the default when {@code auth} is omitted). */
    private static final String AUTH_AUTO = "auto";
    /** Public / unauthenticated access. */
    private static final String AUTH_ANONYMOUS = "anonymous";
    /** A stored long-lived secret (access key + secret, service-account JSON, account key, SAS, ...). */
    private static final String AUTH_STATIC_CREDENTIALS = "static_credentials";
    /** Issuer-minted OIDC federation, configured via federated settings. */
    private static final String AUTH_FEDERATED_IDENTITY = "federated_identity";
    /** The node's ambient cloud identity (instance profile / IMDS / metadata server). */
    private static final String AUTH_MANAGED_IDENTITY = "managed_identity";

    /** Deprecated former name for {@link #AUTH_ANONYMOUS}. */
    private static final String AUTH_DEPRECATED_NONE = "none";
    /** Deprecated former name for {@link #AUTH_MANAGED_IDENTITY}. */
    private static final String AUTH_DEPRECATED_WORKLOAD_IDENTITY = "workload_identity";

    /**
     * Maps each deprecated {@code auth} value name (the entry key) to its canonical replacement (the entry value).
     * Still accepted for backwards compatibility; {@link #normalize(Map)} rewrites the key to the value on parse and
     * emits a deprecation warning per use.
     */
    private static final Map<String, String> DEPRECATED_AUTH_ALIASES = Map.ofEntries(
        Map.entry(AUTH_DEPRECATED_NONE, AUTH_ANONYMOUS),
        Map.entry(AUTH_DEPRECATED_WORKLOAD_IDENTITY, AUTH_MANAGED_IDENTITY)
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

    /** Like {@link #FileDataSourceConfiguration(Map, Map)}, but for a PUT-as-update; see
     *  {@link DataSourceConfiguration#DataSourceConfiguration(Map, Map, Set)}. */
    protected FileDataSourceConfiguration(
        Map<String, Object> raw,
        Map<String, DataSourceConfigDefinition> fieldDefs,
        Set<String> preexistingSecretKeys
    ) {
        super(raw, fieldDefs, preexistingSecretKeys);
    }

    @Override
    protected void normalize(Map<String, Object> parsed) {
        Object value = parsed.get(AUTH.name());
        if (value instanceof String s) {
            String canonical = DEPRECATED_AUTH_ALIASES.get(s);
            if (canonical != null) {
                deprecationLogger.warn(DeprecationCategory.API, DEPRECATED_AUTH_LOG_KEY_PREFIX + s, DEPRECATED_AUTH_MESSAGE, s, canonical);
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
        boolean federatedAuth = hasFederatedAuth();
        if (isAnonymous()) {
            if (secrets) {
                errors.addValidationError(
                    "auth=anonymous cannot be combined with " + credentialSource() + "; anonymous access uses no credentials"
                );
            }
            if (federatedAuth) {
                errors.addValidationError(
                    "auth=anonymous cannot be combined with federated authentication settings; anonymous access uses no credentials"
                );
            }
        }
        if (isManagedIdentity()) {
            if (secrets) {
                errors.addValidationError("auth=managed_identity cannot be combined with " + credentialSource());
            }
            if (federatedAuth) {
                errors.addValidationError("auth=managed_identity cannot be combined with federated authentication settings");
            }
        }
        if (isStaticCredentials()) {
            // A complete static credential is required — a partial secret set (e.g. an S3 session_token with no
            // access/secret key) would otherwise pass here and only fail at provider-build time. Mirrors the
            // per-mode completeness that federated_identity gets via validateCredentials().
            if (hasCredentials() == false) {
                errors.addValidationError("auth=static_credentials requires complete explicit credentials");
            }
            if (federatedAuth) {
                errors.addValidationError("auth=static_credentials cannot be combined with federated authentication settings");
            }
        }
        if (isFederatedIdentity()) {
            if (federatedAuth == false) {
                errors.addValidationError("auth=federated_identity requires federated authentication settings");
            }
            if (secrets) {
                errors.addValidationError("auth=federated_identity cannot be combined with " + credentialSource());
            }
        }
        // auto (or omitted) resolves the mode from the fields present; reject a config that resolves to nothing here
        // rather than deferring the failure to query time. Uses the same predicates resolveAuthMode() infers on
        // (a complete credential or federated settings), so a config that passes validation is always resolvable.
        if (isAuto()) {
            if (hasCredentials() == false && federatedAuth == false) {
                errors.addValidationError(unresolvedAuthMessage());
            }
            // The per-mode branches above already forbid secret+federated for every explicit mode; auto is the only
            // path where the two can arrive together, so the conflict check is scoped here.
            if (secrets && federatedAuth) {
                errors.addValidationError(credentialSource() + " cannot be combined with federated authentication settings");
            }
        }
        validateCredentials(errors);
    }

    /**
     * Names the source of the credentials that {@link #hasAnySecretValue()} found, for a conflict message: this
     * request ("explicit credentials") or, on a PUT-as-update where the request itself supplies none, the
     * existing stored data source. A message built from the latter also points at how to actually clear them,
     * since simply omitting the secret field again would just carry it forward again.
     */
    private String credentialSource() {
        if (hasAnyExplicitSecretValue()) {
            return "explicit credentials";
        }
        return "credentials carried over from the stored data source (send the secret field(s) as null to clear them)";
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

    /** Returns true when the mode is unspecified — {@code auth=auto} or omitted — so it is resolved from the fields present. */
    private boolean isAuto() {
        String auth = auth();
        return auth == null || AUTH_AUTO.equals(auth);
    }

    /** The resolved credential-selection mode a provider builds a mechanism for. */
    public enum AuthMode {
        ANONYMOUS,
        STATIC_CREDENTIALS,
        FEDERATED_IDENTITY,
        MANAGED_IDENTITY
    }

    /**
     * Resolves the credential-selection mode. An explicit {@code auth} value maps straight to its mode — no field
     * inference. Only {@code auto}/omitted infers the mode from the fields present, which is the single inference
     * site in the datasource stack. Returns {@code null} only for an unresolvable {@code auto} config (no complete
     * credential and no federated settings); {@link #validate} rejects that at create time, so any validated config
     * always resolves to a non-null mode.
     */
    public AuthMode resolveAuthModeOrNull() {
        if (isAnonymous()) {
            return AuthMode.ANONYMOUS;
        }
        if (isStaticCredentials()) {
            return AuthMode.STATIC_CREDENTIALS;
        }
        if (isFederatedIdentity()) {
            return AuthMode.FEDERATED_IDENTITY;
        }
        if (isManagedIdentity()) {
            return AuthMode.MANAGED_IDENTITY;
        }
        // auto or omitted — the only place field inference happens. Guarding on isAuto() means a bogus/unsupported
        // auth value (already rejected by validate()) falls through to null rather than silently inferring a mode from
        // the fields present; defense-in-depth.
        if (isAuto()) {
            if (hasFederatedAuth()) {
                return AuthMode.FEDERATED_IDENTITY;
            }
            if (hasCredentials()) {
                return AuthMode.STATIC_CREDENTIALS;
            }
        }
        return null;
    }

    /**
     * The resolved mode, for providers to switch on. Throws for the unresolvable-{@code auto} case, which is
     * unreachable for a config that passed {@link #validate} (defense-in-depth).
     */
    public AuthMode resolveAuthMode() {
        AuthMode mode = resolveAuthModeOrNull();
        if (mode == null) {
            throw new IllegalArgumentException(unresolvedAuthMessage());
        }
        return mode;
    }

    /**
     * Per-CSP message naming the credential fields that would make an {@code auto} config resolvable. Used by
     * {@link #validate} for the create-time rejection and by {@link #resolveAuthMode}'s defensive throw.
     */
    public abstract String unresolvedAuthMessage();

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
