/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity.spi;

import org.elasticsearch.action.ActionListener;

import java.time.Instant;
import java.util.Objects;

/**
 * Client for the workload-identity-issuer service, which mints short-lived OIDC JWTs that
 * Elasticsearch exchanges with a cloud provider's workload identity federation for access to
 * customer-owned resources.
 *
 * <p>This interface is the public extension surface of the workload-identity module: it lives
 * in {@code modules/workload-identity/spi} and is published to extending plugins through the
 * plugin bundle's {@code spi/} directory, while the Apache HttpComponents transport that
 * implements it stays private to the module's own classloader. Plugins that depend on
 * workload-identity should declare {@code extendedPlugins = ['workload-identity']} and
 * compile against this jar only.
 *
 * <p>Implementations connect to the issuer's HTTPS token endpoint using mTLS and are safe for
 * concurrent use by multiple threads. Resource ownership (HTTP client, IO reactor, SSL config)
 * lives with the workload-identity plugin, which closes those components on node shutdown;
 * the client itself does not need to be closed.
 */
public interface WorkloadIdentityIssuerClient {

    /**
     * Asynchronously request a workload-identity JWT for the supplied {@link IssueTokenRequest}.
     *
     * <p>The listener is invoked exactly once, either with an {@link IssueTokenResponse} carrying
     * the signed JWT, or with a failure. The failure may be one of:
     * <ul>
     *     <li>{@link java.io.IOException} for transport-level problems (DNS, TLS handshake,
     *     socket I/O, mTLS rejection by the proxy);</li>
     *     <li>{@link WorkloadIdentityIssuerException} for non-2xx HTTP responses or malformed
     *     responses from the issuer; the {@link WorkloadIdentityIssuerException#statusCode()}
     *     reflects the HTTP status when applicable;</li>
     *     <li>{@link WorkloadIdentityNotEnabledException} when the workload-identity feature is
     *     not enabled on this node (see {@link #isEnabled()});</li>
     *     <li>{@link IllegalArgumentException} for invalid arguments detected synchronously.</li>
     * </ul>
     */
    void issueToken(IssueTokenRequest request, ActionListener<IssueTokenResponse> listener);

    /**
     * Activation predicate for the workload-identity feature on this node. The module is always
     * loaded; this method indicates whether it has been activated by configuration. When
     * {@code false}, every call to {@link #issueToken} fails the listener with
     * {@link WorkloadIdentityNotEnabledException}, letting consumers gate workload-identity-backed
     * code paths and fall back to alternative auth where applicable.
     *
     * @return {@code true} when this client can issue tokens.
     */
    default boolean isEnabled() {
        return true;
    }

    /**
     * Parameters for a single {@code POST /token} request. Only {@code audience} is caller-supplied;
     * the issuer derives the other JWT claims from the mTLS connection.
     *
     * @param audience the {@code aud} claim to embed in the JWT, identifying the cloud-provider
     *                 resource that will validate it (e.g. an AWS IAM role ARN).
     */
    record IssueTokenRequest(String audience) {
        public IssueTokenRequest {
            Objects.requireNonNull(audience, "audience must not be null");
            if (audience.isEmpty()) {
                throw new IllegalArgumentException("audience must not be empty");
            }
        }
    }

    /**
     * Successful response from the {@code POST /token} endpoint.
     *
     * @param token     the signed JWT in compact serialization form (header.payload.signature)
     * @param expiresAt the {@code exp} claim value, as an instant
     */
    record IssueTokenResponse(String token, Instant expiresAt) {
        public IssueTokenResponse {
            Objects.requireNonNull(token, "token must not be null");
            Objects.requireNonNull(expiresAt, "expiresAt must not be null");
            if (token.isEmpty()) {
                throw new IllegalArgumentException("token must not be empty");
            }
        }
    }

    /**
     * Failure raised when the issuer responds with a non-success HTTP status, or with a body that
     * does not match the expected JSON shape. Network-level failures (TLS, connect, read) are
     * raised as plain {@link java.io.IOException} via the listener.
     */
    final class WorkloadIdentityIssuerException extends RuntimeException {

        private static final int NO_STATUS_CODE = -1;

        private final int statusCode;

        public WorkloadIdentityIssuerException(String message) {
            this(message, NO_STATUS_CODE, null);
        }

        public WorkloadIdentityIssuerException(String message, int statusCode) {
            this(message, statusCode, null);
        }

        public WorkloadIdentityIssuerException(String message, int statusCode, Throwable cause) {
            super(message, cause);
            this.statusCode = statusCode;
        }

        /**
         * @return the HTTP status returned by the issuer, or {@code -1} when the failure was not
         *         a transport-level HTTP response (e.g. malformed body, unsupported response shape).
         */
        public int statusCode() {
            return statusCode;
        }
    }

    /**
     * Failure raised when {@link #issueToken} is called but the workload-identity feature is not
     * enabled on this node (see {@link #isEnabled()}). Distinct from {@link WorkloadIdentityIssuerException}
     * because the failure originates in local configuration, not in the issuer's response, and
     * consumers may want to handle it differently (e.g. fall back to an alternative auth path).
     */
    final class WorkloadIdentityNotEnabledException extends RuntimeException {
        public WorkloadIdentityNotEnabledException(String message) {
            super(message);
        }
    }
}
