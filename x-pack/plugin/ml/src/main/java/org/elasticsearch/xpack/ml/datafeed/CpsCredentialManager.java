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
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.action.Grant;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
 *   <li>At deletion: revoke the internal API key via the invalidate API</li>
 * </ul>
 * <p>
 * This class follows the same patterns as the transform's {@code CrossProjectHeadersHelper}.
 */
public class CpsCredentialManager {

    private static final Logger logger = LogManager.getLogger(CpsCredentialManager.class);

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

    // Secure setting for the Elasticsearch shared service secret (X-Client-Authentication).
    // In serverless, this is configured as a secure setting.
    // TODO: Wire up from actual secure settings when available. For now, read from thread context or settings.
    public static final Setting<String> SHARED_SERVICE_SECRET_SETTING = Setting.simpleString(
        "xpack.ml.cps.shared_service_secret",
        Setting.Property.NodeScope,
        Setting.Property.Filtered
    );

    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final SecureString sharedServiceSecret;

    public CpsCredentialManager(Client client, ThreadPool threadPool, ClusterService clusterService, Settings settings) {
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        // Read shared service secret from settings. In production serverless this comes from secure settings.
        String secret = SHARED_SERVICE_SECRET_SETTING.get(settings);
        this.sharedServiceSecret = secret.isEmpty() ? null : new SecureString(secret.toCharArray());
    }

    /**
     * Result of granting an internal API key.
     *
     * @param apiKeyId          The API key ID
     * @param encodedCredential The Base64-encoded API key credential (id:key)
     * @param authHeaders       Persistable security headers captured after re-authentication with the internal key
     */
    public record InternalApiKeyResult(String apiKeyId, String encodedCredential, Map<String, String> authHeaders) {}

    /**
     * Grants an internal universal API key for a CPS-enabled datafeed.
     * <p>
     * Flow (per TDD "GrantInternalUniversalApiKey"):
     * <ol>
     *   <li>Read original UIAM credential from ThreadContext</li>
     *   <li>Call {@code GrantApiKeyAction} with the credential and shared service secret</li>
     *   <li>Re-authenticate using the new internal key to obtain auth headers</li>
     *   <li>Return the key credential and auth headers for persistence</li>
     * </ol>
     *
     * @param datafeedId The datafeed ID (used for naming the API key)
     * @param listener   Returns {@link InternalApiKeyResult} on success
     */
    public void grantInternalApiKey(String datafeedId, ActionListener<InternalApiKeyResult> listener) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        final String authenticatingToken = threadContext.getHeader(AUTHENTICATING_TOKEN_HEADER);

        if (authenticatingToken == null) {
            listener.onFailure(
                new IllegalStateException(
                    "Cannot create internal API key for CPS datafeed ["
                        + datafeedId
                        + "]: no UIAM authenticating token found in thread context"
                )
            );
            return;
        }

        if (sharedServiceSecret == null) {
            listener.onFailure(
                new IllegalStateException(
                    "Cannot create internal API key for CPS datafeed [" + datafeedId + "]: shared service secret is not configured"
                )
            );
            return;
        }

        // Build the grant request using the UIAM authenticating token
        GrantApiKeyRequest grantRequest = new GrantApiKeyRequest();
        Grant grant = grantRequest.getGrant();
        grant.setType(Grant.ACCESS_TOKEN_GRANT_TYPE);
        grant.setAccessToken(new SecureString(authenticatingToken.toCharArray()));
        grant.setClientAuthentication(new Grant.ClientAuthentication(sharedServiceSecret));

        // Configure the API key to create
        CreateApiKeyRequest apiKeyRequest = grantRequest.getApiKeyRequest();
        apiKeyRequest.setName("ml-datafeed-" + datafeedId);
        apiKeyRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        // No expiration for internal API keys (initial iteration per TDD)
        // No role descriptors - inherits caller roles

        logger.debug("[{}] Granting internal API key for CPS datafeed", datafeedId);

        client.execute(GrantApiKeyAction.INSTANCE, grantRequest, ActionListener.wrap(createApiKeyResponse -> {
            String apiKeyId = createApiKeyResponse.getId();
            String encodedCredential = encodeApiKey(apiKeyId, createApiKeyResponse.getKey());

            logger.info("[{}] Internal API key created (key_id={})", datafeedId, apiKeyId);

            // Re-authenticate using the new internal key to obtain auth headers.
            // This ensures the stored auth_info reflects the internal API key identity,
            // not the original user credential (which may be ephemeral, e.g. session token).
            reAuthenticateWithInternalKey(datafeedId, apiKeyId, encodedCredential, listener);
        }, e -> {
            logger.error("[{}] Failed to grant internal API key for CPS datafeed", datafeedId, e);
            listener.onFailure(e);
        }));
    }

    /**
     * Re-authenticates using the newly created internal API key and captures
     * persistable security headers that represent the internal key's identity.
     */
    private void reAuthenticateWithInternalKey(
        String datafeedId,
        String apiKeyId,
        String encodedCredential,
        ActionListener<InternalApiKeyResult> listener
    ) {
        // Stash current context and set up the internal API key as the authenticating credential.
        // Use ContextPreservingActionListener so the stashed API-key context is available in callbacks.
        final ThreadContext threadContext = threadPool.getThreadContext();
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", "ApiKey " + encodedCredential);

            client.execute(
                AuthenticateAction.INSTANCE,
                AuthenticateRequest.INSTANCE,
                new ContextPreservingActionListener<>(supplier, ActionListener.wrap(authenticateResponse -> {
                    // Capture persistable headers from the current context (now authenticated as the internal API key)
                    Map<String, String> authHeaders = ClientHelper.getPersistableSafeSecurityHeaders(
                        threadPool.getThreadContext(),
                        clusterService.state()
                    );

                    logger.debug(
                        "[{}] Re-authenticated with internal API key (key_id={}), captured {} auth headers",
                        datafeedId,
                        apiKeyId,
                        authHeaders.size()
                    );

                    listener.onResponse(new InternalApiKeyResult(apiKeyId, encodedCredential, authHeaders));
                }, e -> {
                    logger.error("[{}] Re-authentication with internal API key (key_id={}) failed", datafeedId, apiKeyId, e);
                    listener.onFailure(e);
                }))
            );
        }
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

        logger.debug("[{}] Revoking internal API key (key_id={})", datafeedId, apiKeyId);

        InvalidateApiKeyRequest request = InvalidateApiKeyRequest.usingApiKeyId(apiKeyId, false);

        // Execute with ML_ORIGIN since we need internal permissions to invalidate the key.
        // Use ContextPreservingActionListener so the stashed context is available in callbacks.
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

    /**
     * Encodes an API key ID and secret into the Base64-encoded format used for API key authentication.
     */
    private static String encodeApiKey(String id, SecureString key) {
        String rawValue = id + ":" + key;
        return Base64.getEncoder().encodeToString(rawValue.getBytes(StandardCharsets.UTF_8));
    }
}
