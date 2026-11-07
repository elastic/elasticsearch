/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity.spi;

import org.elasticsearch.core.Nullable;

import java.time.Instant;
import java.util.Objects;

/**
 * Receiver for audit events on the ES|QL data source path: per-query data source access (all
 * auth modes) and the workload-identity token mints backing federated ("keyless") access.
 *
 * <p>Neither event is visible to the transport-action audit machinery: token minting has no
 * inbound principal, and which data sources a query exercised is known only to ES|QL at
 * completion. This listener lets the emitters hand events to whichever plugin owns the audit
 * trail without a dependency in that direction.
 *
 * <p>Implementations must be thread-safe and must not throw. Events never carry token material.
 */
public interface DataSourceAuditListener {

    /** A listener that discards every event; installed while no audit consumer has registered. */
    DataSourceAuditListener NOOP = new DataSourceAuditListener() {
        @Override
        public void tokenIssued(TokenIssuance issuance) {}

        @Override
        public void tokenIssuanceFailed(TokenIssuanceFailure failure) {}

        @Override
        public void dataSourceAccess(DataSourceAccess access) {}
    };

    /**
     * A workload-identity JWT was minted: one event per real fetch (first use or re-fetch after
     * eviction), never for cache hits. Credential <em>use</em> is audited via
     * {@link #dataSourceAccess}.
     */
    void tokenIssued(TokenIssuance issuance);

    /** A JWT mint failed after the retry budget: one event per failed fetch, not per attempt. */
    void tokenIssuanceFailed(TokenIssuanceFailure failure);

    /**
     * A query accessed (or failed to access) a registered data source: one event per data source
     * per query, any auth mode, emitted at query completion. Implementations read the
     * authenticated user from the calling thread's context.
     */
    void dataSourceAccess(DataSourceAccess access);

    /**
     * A successful JWT mint.
     *
     * @param audience  the requested {@code aud} claim
     * @param issuer    the issuer endpoint the token was fetched from
     * @param subject   the decoded {@code sub} claim (e.g. {@code deployment:abc}), or {@code null}
     * @param sessionId the decoded {@code jti} claim (per-token session id), or {@code null}
     * @param expiresAt the issuer-reported expiry
     */
    record TokenIssuance(String audience, String issuer, @Nullable String subject, @Nullable String sessionId, Instant expiresAt) {
        public TokenIssuance {
            Objects.requireNonNull(audience, "audience must not be null");
            Objects.requireNonNull(issuer, "issuer must not be null");
            Objects.requireNonNull(expiresAt, "expiresAt must not be null");
        }
    }

    /**
     * A failed JWT mint.
     *
     * @param audience the requested {@code aud} claim
     * @param issuer   the issuer endpoint the fetch was sent to
     * @param cause    the terminal failure
     */
    record TokenIssuanceFailure(String audience, String issuer, Exception cause) {
        public TokenIssuanceFailure {
            Objects.requireNonNull(audience, "audience must not be null");
            Objects.requireNonNull(issuer, "issuer must not be null");
            Objects.requireNonNull(cause, "cause must not be null");
        }
    }

    /**
     * A query's use of one registered data source. The identity fields are federated-only:
     * static credentials are classified secret and never described.
     *
     * @param dataSourceName    the registered data source name
     * @param dataSourceType    the data source type (e.g. {@code s3})
     * @param auth              the resolved auth mode ({@code federated_identity},
     *                          {@code static_credentials}, {@code managed_identity}, {@code anonymous})
     * @param identity          the federated target identity (e.g. an IAM role ARN), or {@code null}
     * @param audience          the effective federated {@code jwt_audience}, joining this event to
     *                          {@link TokenIssuance} events, or {@code null}
     * @param sessionId         best-effort session id of the cached token for {@code audience} at
     *                          completion time, or {@code null}
     * @param granted           {@code true} when the credential phase was not the cause of failure;
     *                          {@code false} when it was
     * @param credentialFailure the credential-phase failure when not granted; {@code null} otherwise
     */
    record DataSourceAccess(
        String dataSourceName,
        String dataSourceType,
        String auth,
        @Nullable String identity,
        @Nullable String audience,
        @Nullable String sessionId,
        boolean granted,
        @Nullable Exception credentialFailure
    ) {
        public DataSourceAccess {
            Objects.requireNonNull(dataSourceName, "dataSourceName must not be null");
            Objects.requireNonNull(dataSourceType, "dataSourceType must not be null");
            Objects.requireNonNull(auth, "auth must not be null");
            if (granted == false && credentialFailure == null) {
                throw new IllegalArgumentException("credentialFailure must be provided when access was not granted");
            }
            if (granted && credentialFailure != null) {
                throw new IllegalArgumentException("credentialFailure must be null when access was granted");
            }
        }
    }
}
