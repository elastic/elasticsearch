/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialManager;
import org.elasticsearch.xpack.core.security.cloud.InternalCloudApiKeyService;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

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
     * Returns a client wrapped with the caller's cloud credential from the thread context,
     * or the original client if the feature flag is off or no credential is present.
     * Prefers the user's secondary authentication when present (e.g. Kibana primary + user secondary)
     * via {@link #currentCallerCredential()}.
     */
    public Client wrapWithUiamIfPresent(Client client) {
        var credential = currentCallerCredential();
        return credential == null ? client : credentialManager.wrapClient(client, credential);
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
        return useSecondaryAuthIfAvailable(securityContext, () -> {
            var threadContext = threadPool.getThreadContext();
            return credentialManager.hasCloudManagedCredential(threadContext)
                ? credentialManager.extractCloudManagedCredential(threadContext)
                : null;
        });
    }

    /**
     * Re-injects a cloud-managed credential into the current thread context. The inverse of
     * {@link #currentCallerCredential()}, used by the receiving handler of an internal action
     * to restore the caller's credential after the system-origin stash dropped the original
     * transient. No-op when the credential is {@code null} or the feature flag is off, which
     * also keeps callers safe against the no-op {@link CloudCredentialManager} implementation.
     */
    public void injectCallerCredential(@Nullable CloudCredential credential) {
        if (credential != null && TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled()) {
            credentialManager.injectCloudManagedCredential(threadPool.getThreadContext(), credential);
        }
    }

    /**
     * If the current thread context carries a cloud-managed credential, mints a new internal API key
     * from it and persists the result. If no cloud credential is present, performs a best-effort
     * revoke + cleanup of any previously stored credential for the given transform.
     *
     * <p><b>SecureString consumption contract:</b>
     * {@link InternalCloudApiKeyService#grantCloudAuthentication} consumes the input credential's
     * {@code SecureString} synchronously before it returns (the serverless implementation
     * deserializes the bytes into a {@code CloudToken} on the calling thread, then dispatches the
     * UIAM request asynchronously using the token). Closing {@code callerCredential} in the async
     * listener is therefore safe and does not race the in-flight grant call.
     *
     * @param transformId the transform id
     * @param listener    called with {@code null} on completion (success or cleanup)
     */
    public void mintAndPersist(String transformId, ActionListener<Void> listener) {
        // Extract the caller credential up front, under the user's secondary auth when present.
        // The subsequent prior-credential load is async, and by the time its listener fires the
        // secondary-auth stash is long gone, so we can't re-extract from the thread context later.
        CloudCredential callerCredential = currentCallerCredential();
        if (callerCredential == null) {
            logger.debug("[{}] no cloud credential in thread context, revoking + cleaning up any stored credential", transformId);
            loadRevokeAndDelete(transformId, listener);
            return;
        }

        // Detect rekey vs first-mint: load the prior id (if any) before granting. The prior
        // credential itself is revoked later by the deferred-swap path; we only need its id for the
        // audit row here, so we close immediately and let the swap path do the actual revoke.
        configManager.getTransformCloudCredential(
            transformId,
            true,
            ActionListener.releaseAfter(listener.delegateFailureAndWrap((l, prior) -> {
                String priorId = prior == null ? null : prior.id();
                Releasables.close(prior);
                mintAndPersistAfterPriorLoad(transformId, callerCredential, priorId, l);
            }), callerCredential)
        );
    }

    private void mintAndPersistAfterPriorLoad(
        String transformId,
        CloudCredential callerCredential,
        @Nullable String priorId,
        ActionListener<Void> listener
    ) {
        logger.debug("[{}] minting internal cloud API key from caller credential", transformId);

        apiKeyService.grantCloudAuthentication(
            callerCredential,
            "transform:" + transformId,
            listener.delegateFailureAndWrap((l, grantResult) -> {
                var persisted = grantResult.persistedCredential();
                logger.debug("[{}] granted cloud API key [{}], persisting", transformId, persisted.id());

                configManager.putTransformCloudCredential(
                    transformId,
                    persisted,
                    ActionListener.releaseAfter(l.delegateFailureAndWrap((ll, success) -> {
                        if (priorId != null) {
                            auditor.info(transformId, "rotated cloud credential, new [" + persisted.id() + "], previous [" + priorId + "]");
                        } else {
                            auditor.info(transformId, "minted cloud credential [" + persisted.id() + "]");
                        }
                        ll.onResponse(null);
                    }), persisted)
                );
            })
        );
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
     * Loads the persisted cloud credential for {@code transformId} from storage and revokes it
     * (best-effort, via {@link #revokeAndClose}). Always notifies {@code listener} with {@code null}
     * on completion — load failures are logged but never propagate because the idempotent UIAM
     * revoke is safe to retry via a future GC sweep. Callers chain the next step (e.g. delete the
     * full transform) off the listener. No-op when the feature flag is off.
     *
     * <p>Use this when the caller will subsequently remove the credential storage doc itself
     * (e.g. via a transform-wide DBQ in {@code deleteTransform}). For compensating cleanup paths
     * where the caller owns the credential-doc delete too, use {@link #loadRevokeAndDelete}.
     */
    public void loadAndRevoke(String transformId, ActionListener<Void> listener) {
        if (TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled() == false) {
            listener.onResponse(null);
            return;
        }
        configManager.getTransformCloudCredential(transformId, true, ActionListener.wrap(credential -> {
            revokeAndClose(transformId, credential);
            listener.onResponse(null);
        }, e -> {
            logger.warn(() -> "[" + transformId + "] failed to load cloud credential during revoke; proceeding", e);
            listener.onResponse(null);
        }));
    }

    /**
     * Loads + revokes (via {@link #loadAndRevoke}) and then removes the credential storage doc for
     * {@code transformId}. Best-effort throughout: a load or revoke failure is logged but the
     * storage delete still runs; a delete failure is logged but the listener still completes. No-op
     * when the feature flag is off.
     *
     * <p>Use this in compensating-cleanup paths where the caller owns the credential-doc delete
     * (e.g. {@code TransportPutTransformAction.putTransform}'s config-write-failure branch).
     */
    public void loadRevokeAndDelete(String transformId, ActionListener<Void> listener) {
        if (TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled() == false) {
            // The storage doc only exists when the feature is on, so skip both load+revoke and the
            // storage DBQ entirely. Matches the no-op contract of loadAndRevoke + revokeAndClose.
            listener.onResponse(null);
            return;
        }
        loadAndRevoke(
            transformId,
            ActionListener.running(() -> configManager.deleteTransformCloudCredential(transformId, ActionListener.wrap(deleted -> {
                logger.trace("[{}] cleanup of stored credential: deleted={}", transformId, deleted);
                listener.onResponse(null);
            }, deleteFailure -> {
                logger.warn(() -> "[" + transformId + "] cloud credential storage delete failed", deleteFailure);
                listener.onResponse(null);
            })))
        );
    }
}
