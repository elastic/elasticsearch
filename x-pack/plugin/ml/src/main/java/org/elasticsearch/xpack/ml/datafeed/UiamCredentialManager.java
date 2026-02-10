/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Manages UIAM internal API key lifecycle for CPS-enabled datafeeds.
 * <p>
 * Follows the design from the "Elasticsearch Security for Cross-Project ML TDD":
 * <ul>
 *   <li>At creation: mint an internal universal API key via the UIAM {@code _grant} API</li>
 *   <li>At execution: the stored internal API key's auth headers enable CPS token refresh</li>
 *   <li>At deletion: revoke the internal API key via the UIAM revoke API</li>
 * </ul>
 * <p>
 * This class follows the same patterns as the transform's {@code CrossProjectHeadersHelper}.
 */
public class UiamCredentialManager {

    private static final Logger logger = LogManager.getLogger(UiamCredentialManager.class);

    /**
     * Header name for the UIAM authenticating token in ThreadContext.
     * This is the original UIAM credential (e.g. universal API key or session token).
     */
    static final String AUTHENTICATING_TOKEN_HEADER = "_security_serverless_authenticating_token";

    /**
     * Header name for the request-scoped credential in ThreadContext.
     * This short-lived token is minted by the CPS infrastructure for cross-project actions.
     */
    static final String REQUEST_SCOPED_CREDENTIAL_HEADER = "_security_serverless_request_scoped_credential";

    private final Client client;
    private final ThreadPool threadPool;

    public UiamCredentialManager(Client client, ThreadPool threadPool) {
        this.client = client;
        this.threadPool = threadPool;
    }

    /**
     * Result of granting an internal API key.
     *
     * @param apiKeyId          The UIAM internal API key ID
     * @param encodedCredential The encoded internal API key credential
     * @param authHeaders       Persistable security headers captured after re-authentication with the internal key
     */
    public record InternalApiKeyResult(String apiKeyId, String encodedCredential, Map<String, String> authHeaders) {}

    /**
     * Grants an internal universal API key for a CPS-enabled datafeed.
     * <p>
     * Per the TDD "GrantInternalUniversalApiKey", this should:
     * <ol>
     *   <li>Read original UIAM credential from ThreadContext</li>
     *   <li>Exchange it for an internal universal API key via the UIAM {@code _grant} API</li>
     *   <li>Re-authenticate using the new internal key to obtain persistable auth headers</li>
     *   <li>Return the key credential and auth headers for persistence</li>
     * </ol>
     * <p>
     * <b>NOTE:</b> The UIAM {@code _grant} API produces a UIAM internal universal API key (a cross-project
     * credential managed by UIAM), which is fundamentally different from an Elasticsearch API key (project-local).
     * The security team needs to provide a service or transport action that wraps the UIAM {@code _grant} call.
     * See the "Common tooling and primitives" section of the TDD for details.
     *
     * @param datafeedId The datafeed ID (used for naming the API key)
     * @param listener   Returns {@link InternalApiKeyResult} on success
     */
    public void grantInternalApiKey(String datafeedId, ActionListener<InternalApiKeyResult> listener) {
        // TODO: Implement GrantInternalUniversalApiKey using the UIAM _grant API.
        //
        // The UIAM _grant API is an external UIAM service endpoint that mints internal universal API keys
        // (cross-project credentials), NOT Elasticsearch API keys (project-local). The security team needs
        // to provide either:
        // 1. A new injectable service that wraps the UIAM _grant HTTP call, or
        // 2. A new internal-only transport action
        //
        // The grant flow per TDD:
        // - Input: original UIAM credential (from ThreadContext) + ES shared service secret
        // - Call UIAM _grant with X-Client-Authentication header containing the shared service secret
        // - Always use the ES shared service secret, not Kibana's
        // - Re-authenticate using the new internal key to obtain the authentication instance
        // - Output: internal API key credential + authentication info (persistable headers)
        //
        // Secondary authentication:
        // - If es-secondary-authorization header is present, grant under the secondary identity
        // - Primary identity is typically Kibana service account; secondary is end user UIAM credential
        //
        // Failures are hard failures and must abort datafeed creation.
        //
        // See: "Elasticsearch Security for Cross-Project ML TDD" sections:
        // - "GrantInternalUniversalApiKey" (Common tooling and primitives)
        // - "Transform creation" (Proposed flow, steps 1-4)
        // - "Secondary authentication handling"
        listener.onFailure(
            new UnsupportedOperationException(
                "GrantInternalUniversalApiKey is not yet implemented for CPS datafeed ["
                    + datafeedId
                    + "]. "
                    + "The UIAM _grant API integration must be provided by the security team. "
                    + "See the 'Elasticsearch Security for Cross-Project ML TDD' for details."
            )
        );
    }

    /**
     * Revokes an internal API key. Used during datafeed deletion or update (re-keying).
     * Failures are logged but do not block the calling operation.
     *
     * @param apiKeyId   The API key ID to revoke
     * @param datafeedId The datafeed ID (for logging)
     * @param listener   Called with {@code true} on success, {@code false} on failure (never fails the listener)
     */
    public void revokeApiKey(String apiKeyId, String datafeedId, ActionListener<Boolean> listener) {
        if (apiKeyId == null) {
            listener.onResponse(false);
            return;
        }

        // TODO: Replace with UIAM RevokeInternalUniversalApiKey once the security team provides the primitive.
        // The TDD specifies that revocation should use the UIAM revoke API, not the ES InvalidateApiKey API.
        // For now, use the ES InvalidateApiKey API as a placeholder that works for local-only testing.

        logger.debug("[{}] Revoking internal API key (key_id={})", datafeedId, apiKeyId);

        InvalidateApiKeyRequest request = InvalidateApiKeyRequest.usingApiKeyId(apiKeyId, false);

        final ThreadContext threadContext = threadPool.getThreadContext();
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(ClientHelper.ML_ORIGIN)) {
            client.execute(
                InvalidateApiKeyAction.INSTANCE,
                request,
                new ContextPreservingActionListener<>(supplier, ActionListener.wrap(response -> {
                    if (response.getInvalidatedApiKeys().isEmpty() == false) {
                        logger.info("[{}] Internal API key revoked (key_id={})", datafeedId, apiKeyId);
                        listener.onResponse(true);
                    } else {
                        logger.warn(
                            "[{}] Internal API key not found for revocation (key_id={}). "
                                + "Key may have already been revoked or expired.",
                            datafeedId,
                            apiKeyId
                        );
                        listener.onResponse(false);
                    }
                }, e -> {
                    logger.warn(
                        "[{}] Failed to revoke internal API key (key_id={}): {}. " + "Key may need manual cleanup.",
                        datafeedId,
                        apiKeyId,
                        e.getMessage()
                    );
                    // Don't fail the listener - revocation failures should not block deletion/update
                    listener.onResponse(false);
                }))
            );
        }
    }

    /**
     * Checks whether the current thread context contains a UIAM authenticating token,
     * indicating that the request was made with a UIAM credential (not a stack credential).
     */
    public boolean hasUiamCredential() {
        return threadPool.getThreadContext().getHeader(AUTHENTICATING_TOKEN_HEADER) != null;
    }

    /**
     * Returns a copy of the provided headers with the stale request-scoped credential removed.
     * This ensures the CPS infrastructure mints a fresh short-lived token for each search,
     * following the same pattern as the transform's {@code CrossProjectHeadersHelper}.
     * <p>
     * If the headers do not contain a request-scoped credential, the original map is returned.
     */
    public static Map<String, String> headersForCpsSearch(Map<String, String> headers) {
        if (headers.containsKey(REQUEST_SCOPED_CREDENTIAL_HEADER) == false) {
            return headers;
        }
        var copy = new HashMap<>(headers);
        copy.remove(REQUEST_SCOPED_CREDENTIAL_HEADER);
        return copy;
    }
}
