/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialManager;
import org.elasticsearch.xpack.core.security.cloud.InternalCloudApiKeyService;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.transform.utils.SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable;

/**
 * Handles UIAM cloud-credential operations for transforms: minting and persisting the caller's
 * cloud credential, exposing the current caller's credential for callers that need to attach it
 * to a request payload (so it survives a system-origin context stash), and wrapping clients
 * for transport-side calls.
 */
public class TransformCloudCredentialManager {

    private static final Logger logger = LogManager.getLogger(TransformCloudCredentialManager.class);

    private final ThreadPool threadPool;
    @Nullable
    private final SecurityContext securityContext;
    private final CloudCredentialManager credentialManager;
    private final InternalCloudApiKeyService apiKeyService;
    private final TransformConfigManager configManager;
    private final TransformAuditor auditor;

    /**
     * @param securityContext used to swap to the user's secondary authentication when present (e.g.
     *                        Kibana proxying a user request); {@code null} when security is disabled,
     *                        in which case all reads happen on the primary thread context.
     */
    public TransformCloudCredentialManager(
        ThreadPool threadPool,
        @Nullable SecurityContext securityContext,
        CloudCredentialManager credentialManager,
        InternalCloudApiKeyService apiKeyService,
        TransformConfigManager configManager,
        TransformAuditor auditor
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.securityContext = securityContext;
        this.credentialManager = Objects.requireNonNull(credentialManager);
        this.apiKeyService = Objects.requireNonNull(apiKeyService);
        this.configManager = Objects.requireNonNull(configManager);
        this.auditor = Objects.requireNonNull(auditor);
    }

    /**
     * Returns {@code client} wrapped with {@code credential} so downstream actions authenticate
     * with the caller's UIAM cloud credential, or the original {@code client} when {@code credential}
     * is {@code null} (feature flag off or no credential present).
     *
     * <p>The caller owns {@code credential}'s {@link org.elasticsearch.common.settings.SecureString}
     * and must keep it open for the lifetime of the returned wrapped client — typically by registering
     * {@link ActionListener#releaseAfter} on the outer listener that terminates the wrapped client's
     * use. See {@link #currentCallerCredential()} for extracting the caller credential from the
     * current thread context.
     */
    public Client wrapWithUiamIfPresent(Client client, @Nullable CloudCredential credential) {
        return credentialManager.wrapClient(client, credential);
    }

    /**
     * Returns {@code client} wrapped with {@code persisted} so downstream actions authenticate
     * with the transform's stored internal cloud credential, or the original {@code client} when
     * {@code persisted} is {@code null}.
     */
    public Client wrapWithPersistedIfPresent(Client client, @Nullable PersistedCloudCredential persisted) {
        return credentialManager.wrapClient(client, persisted);
    }

    /**
     * Converts a {@link PersistedCloudCredential} (internal API key from storage) into a
     * {@link CloudCredential} suitable for attaching to a validate-transform request payload.
     * Closes {@code persisted} after conversion — the returned credential is owned by the caller
     * and must be closed via {@link ActionListener#releaseAfter} on the validate request.
     */
    @Nullable
    public CloudCredential cloudCredentialFromPersisted(@Nullable PersistedCloudCredential persisted) {
        if (persisted == null) {
            return null;
        }
        try {
            return credentialManager.toCloudCredential(persisted);
        } finally {
            persisted.close();
        }
    }

    /**
     * Returns the cloud-managed credential for the caller in the current thread context, or
     * {@code null} if the feature flag is off or the caller is not cloud-managed. Intended for
     * callers that need to attach the credential to an internal action's request payload so it
     * survives the system-origin context stash performed by
     * {@link org.elasticsearch.xpack.core.ClientHelper#executeAsyncWithOrigin}.
     *
     * <p>When the inbound request carries secondary authentication (e.g. Kibana proxying a user
     * request), the extraction runs under the secondary auth context so the returned credential
     * belongs to the user, not to Kibana. The user's {@code CloudToken} is propagated via
     * {@code SecondaryAuthenticator}'s capture of {@code AuthenticationToken}-typed transients.
     */
    @Nullable
    public CloudCredential currentCallerCredential() {
        if (TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled() == false) {
            return null;
        }
        return useSecondaryAuthIfAvailable(
            securityContext,
            () -> credentialManager.extractCloudManagedCredential(threadPool.getThreadContext())
        );
    }

    /**
     * Mints a new internal API key from {@code callerCredential} (when non-null), persists the
     * result, and returns a rewritten copy of {@code config} with the minted credential id stamped
     * and the stored security headers replaced by the minted key's own identity — so the transform
     * runs as its own cloud API key from here on.
     *
     * <p>If {@code callerCredential} is {@code null} (feature flag off or no UIAM context), the
     * method responds with the original {@code config} unchanged. No cleanup of any prior credential
     * is performed — that responsibility belongs to the caller (typically by threading the prior
     * {@link TransformConfig#getCredentialId} through to a {@link #loadRevokeAndDeleteByTokenId}
     * call after the config write succeeds).
     *
     * <p>The credential is supplied by the caller — typically the value carried on a
     * {@code PutTransformAction.Request}/{@code UpdateTransformAction.Request}, extracted on the
     * coordinating node — rather than read from the current thread context here, since this method
     * commonly runs on the master node after the request has been forwarded, by which point the
     * {@code AUTHENTICATING_CLOUD_TOKEN_THREAD_CONTEXT} transient is no longer present.
     *
     * <p><b>Ownership:</b> {@code callerCredential} is a borrowed reference — this method reads it
     * but does not close it. {@link InternalCloudApiKeyService#grantCloudAuthentication} consumes
     * the {@code SecureString} synchronously before it returns, so nothing here needs the credential
     * to remain valid past this call. Closing it remains the responsibility of whichever scope
     * opened it (typically the coordinating-node {@code doExecute} that extracted it from the
     * thread context), or a copy of it (e.g. via {@link CloudCredential#copyOf}) if the caller also
     * hands the same underlying credential to another consumer.
     *
     * @param config           the transform config whose {@code credentialId} and security headers
     *                         will be replaced on a successful mint; returned unchanged when no
     *                         credential is supplied
     * @param callerCredential the caller's cloud credential, or {@code null} when none is available
     * @param minTransportVersion the cluster's minimum transport version, used to rewrite the
     *                         minted {@link Authentication} before encoding it into the stored headers
     * @param listener         called with the config to write — either the original (no mint) or a
     *                         copy with the new credential id and minted authentication in headers
     */
    public void mintAndPersist(
        TransformConfig config,
        @Nullable CloudCredential callerCredential,
        TransportVersion minTransportVersion,
        ActionListener<TransformConfig> listener
    ) {
        if (callerCredential == null) {
            // Feature off or no UIAM context: nothing to mint and nothing to clean up here.
            listener.onResponse(config);
            return;
        }

        var transformId = config.getId();
        logger.debug("[{}] minting internal cloud API key from caller credential", transformId);
        apiKeyService.grantCloudAuthentication(
            callerCredential,
            "transform:" + transformId,
            listener.delegateFailureAndWrap((l, grantResult) -> {
                var persisted = grantResult.persistedCredential();
                logger.debug("[{}] granted cloud API key [{}], persisting", transformId, persisted.id());

                // Rewrite the config before the persist: if encode() throws, only the UIAM grant
                // needs undoing (no storage doc has been written yet). ActionListener.wrap routes
                // success-handler exceptions to the failure consumer, so building configToWrite
                // inside the success callback would trigger revokeAndClose on an already-closed
                // SecureString.
                final TransformConfig configToWrite;
                try {
                    configToWrite = config.withCredentialId(persisted.id())
                        .setHeaders(replaceSecurityHeaders(grantResult.authentication(), config.getHeaders(), minTransportVersion));
                } catch (Exception encodeFailure) {
                    revokeAndClose(transformId, persisted);
                    l.onFailure(encodeFailure);
                    return;
                }

                configManager.putTransformCloudCredential(transformId, persisted, ActionListener.wrap(success -> {
                    // Close the SecureString now that the id has been persisted.
                    try (persisted) {
                        auditor.info(transformId, "minted cloud credential [" + persisted.id() + "]");
                        l.onResponse(configToWrite);
                    }
                }, persistFailure -> {
                    // The UIAM grant succeeded but the storage write failed. Revoke at UIAM so
                    // the token doesn't exist with no config reference and no storage doc.
                    // revokeAndClose owns the close of persisted; callers are still notified of
                    // the original failure and may also attempt their own compensating cleanup,
                    // which is fine because UIAM revoke is idempotent.
                    logger.warn(
                        () -> "[" + transformId + "] persist of cloud credential [" + persisted.id() + "] failed; revoking at UIAM",
                        persistFailure
                    );
                    revokeAndClose(transformId, persisted);
                    l.onFailure(persistFailure);
                }));
            })
        );
    }

    /**
     * Returns {@code headers} with all {@link ClientHelper#SECURITY_HEADER_FILTERS security
     * headers} removed and {@link AuthenticationField#AUTHENTICATION_KEY} set to the encoded
     * identity of the minted cloud API key. Non-security headers (e.g. future trace headers)
     * are carried through untouched.
     *
     * <p>The authentication is rewritten down to the cluster's minimum transport version before
     * encoding. {@link Authentication#encode()} stamps the effective subject's version into the
     * blob, and the minted key is built locally so it carries this node's current version. During
     * a rolling upgrade some nodes are older, and this config is persisted and later read by
     * whichever node runs the transform task — an older node cannot decode a newer-stamped blob.
     * Mirrors {@code ClientHelper#maybeRewriteSingleAuthenticationHeaderForVersion}, which does
     * the same for the caller's headers. Note that
     * {@link Authentication#maybeRewriteForOlderVersion} does not itself check that the target
     * version is older, so the {@code supports} guard is required.
     */
    private static Map<String, String> replaceSecurityHeaders(
        Authentication authentication,
        Map<String, String> headers,
        TransportVersion minTransportVersion
    ) {
        var updated = headers == null ? new HashMap<String, String>() : new HashMap<>(headers);
        updated.keySet().removeAll(ClientHelper.SECURITY_HEADER_FILTERS);
        var safe = minTransportVersion.supports(authentication.getEffectiveSubject().getTransportVersion())
            ? authentication
            : authentication.maybeRewriteForOlderVersion(minTransportVersion);
        try {
            updated.put(AuthenticationField.AUTHENTICATION_KEY, safe.encode());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to encode minted authentication for transform", e);
        }
        return Map.copyOf(updated);
    }

    /**
     * Revokes the given persisted cloud credential with UIAM and closes its {@code SecureString}.
     * Fire-and-forget: failures are logged + audited but not propagated to the caller, because the
     * serverless revoke API is idempotent so a future retry / GC sweep can still clean up. No-op
     * for null credentials and when the {@code TRANSFORM_CROSS_PROJECT} feature flag is off (the
     * {@link InternalCloudApiKeyService.Default} implementation throws
     * {@link UnsupportedOperationException} on revoke).
     *
     * <p>The {@code transformId} is used for the audit row attribution.
     */
    public void revokeAndClose(String transformId, @Nullable PersistedCloudCredential credential) {
        if (credential == null) {
            return;
        }
        if (TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled() == false) {
            credential.close();
            return;
        }
        String credId = credential.id();
        apiKeyService.revokeCloudAuthentication(credential, ActionListener.releaseAfter(ActionListener.wrap(unused -> {
            logger.debug("[{}] revoked cloud credential [{}]", transformId, credId);
            auditor.info(transformId, "revoked cloud credential [" + credId + "]");
        }, e -> {
            logger.warn(() -> "[" + transformId + "] failed to revoke cloud credential [" + credId + "]", e);
            auditor.warning(transformId, "failed to revoke cloud credential [" + credId + "]: " + e.getMessage());
        }), credential));
    }

    /**
     * Revokes the given in-memory persisted credential at UIAM, deletes its storage doc, and closes
     * its {@code SecureString}. Fire-and-forget: failures are logged but not propagated. Use this
     * in the indexer's credential-swap path where the caller already holds the displaced
     * {@link PersistedCloudCredential} object (so no redundant storage read is needed). No-op for
     * null credentials and when the {@code TRANSFORM_CROSS_PROJECT} feature flag is off.
     *
     * @param transformId the owning transform id (audit attribution only)
     * @param credential  the displaced credential to revoke + delete; null is silently ignored
     */
    public void revokeCloseAndDelete(String transformId, @Nullable PersistedCloudCredential credential) {
        if (credential == null) {
            return;
        }
        if (TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled() == false) {
            credential.close();
            return;
        }
        String credId = credential.id();
        apiKeyService.revokeCloudAuthentication(credential, ActionListener.releaseAfter(ActionListener.wrap(unused -> {
            logger.debug("[{}] revoked cloud credential [{}]", transformId, credId);
            auditor.info(transformId, "revoked cloud credential [" + credId + "]");
            configManager.deleteCloudCredentialByTokenId(
                credId,
                ActionListener.wrap(
                    deleted -> logger.trace("[{}] storage cleanup of credential [{}]: deleted={}", transformId, credId, deleted),
                    deleteFailure -> logger.warn(
                        () -> "[" + transformId + "] storage cleanup of credential [" + credId + "] failed",
                        deleteFailure
                    )
                )
            );
        }, e -> {
            logger.warn(() -> "[" + transformId + "] failed to revoke cloud credential [" + credId + "]", e);
            auditor.warning(transformId, "failed to revoke cloud credential [" + credId + "]: " + e.getMessage());
            // Leave the storage doc in place so the startup sweep or delete API can retry.
        }), credential));
    }

    /**
     * Loads the persisted cloud credential for {@code tokenId} from storage and revokes it
     * (best-effort, via {@link #revokeAndClose}). Always notifies {@code listener} with {@code null}
     * on completion — load failures are logged but never propagate because the idempotent UIAM
     * revoke is safe to retry via a future GC sweep. No-op when the feature flag is off or
     * {@code tokenId} is null.
     *
     * <p>Use this when the caller will subsequently remove the credential storage doc itself
     * (e.g. via {@code deleteTransform}'s transform-wide DBQ which also targets credential docs).
     * For compensating cleanup paths where the caller owns the credential-doc delete too, use
     * {@link #loadRevokeAndDeleteByTokenId}.
     *
     * @param transformId the owning transform id (audit attribution only)
     * @param tokenId     the UIAM tokenId of the credential to revoke; null skips the operation
     */
    public void loadAndRevokeByTokenId(String transformId, @Nullable String tokenId, ActionListener<Void> listener) {
        if (tokenId == null || TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled() == false) {
            listener.onResponse(null);
            return;
        }
        configManager.getTransformCloudCredentialByTokenId(tokenId, true, ActionListener.wrap(credential -> {
            revokeAndClose(transformId, credential);
            listener.onResponse(null);
        }, e -> {
            logger.warn(() -> "[" + transformId + "] failed to load cloud credential [" + tokenId + "] during revoke; proceeding", e);
            listener.onResponse(null);
        }));
    }

    /**
     * Loads + revokes (via {@link #loadAndRevokeByTokenId}) and then removes the credential storage
     * doc for {@code tokenId}. Best-effort throughout: a load or revoke failure is logged but the
     * storage delete still runs; a delete failure is logged but the listener still completes. No-op
     * when the feature flag is off or {@code tokenId} is null.
     *
     * <p>Use this in compensating-cleanup paths where the caller owns the credential-doc delete
     * (e.g. config-write-failure rollback in PUT/UPDATE) and in the running-task credential swap
     * path inside the indexer.
     *
     * @param transformId the owning transform id (audit attribution only)
     * @param tokenId     the UIAM tokenId of the credential to revoke + delete; null skips both
     */
    public void loadRevokeAndDeleteByTokenId(String transformId, @Nullable String tokenId, ActionListener<Void> listener) {
        if (tokenId == null || TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled() == false) {
            // The storage doc only exists when the feature is on and a tokenId was minted; either
            // condition false means there's nothing to clean up here.
            listener.onResponse(null);
            return;
        }
        loadAndRevokeByTokenId(
            transformId,
            tokenId,
            ActionListener.running(() -> configManager.deleteCloudCredentialByTokenId(tokenId, ActionListener.wrap(deleted -> {
                logger.trace("[{}] cleanup of stored credential [{}]: deleted={}", transformId, tokenId, deleted);
                listener.onResponse(null);
            }, deleteFailure -> {
                logger.warn(() -> "[" + transformId + "] cloud credential storage delete for [" + tokenId + "] failed", deleteFailure);
                listener.onResponse(null);
            })))
        );
    }
}
