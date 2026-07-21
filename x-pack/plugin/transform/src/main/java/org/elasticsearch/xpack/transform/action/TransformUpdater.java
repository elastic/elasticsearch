/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformIndex;

import java.util.Map;

/**
 * With {@link TransformUpdater} transforms can be updated or upgraded to the latest version
 *
 * This implementation is shared between _update and _upgrade
 */
public class TransformUpdater {

    private static final Logger logger = LogManager.getLogger(TransformUpdater.class);

    public static final class UpdateResult {

        // the status of the update
        public enum Status {
            NONE, // all checks passed, no action taken
            UPDATED, // updated
            NEEDS_UPDATE, // special dry run status
            DELETED // internal status if a transform got deleted during upgrade
        }

        // the new config after the update
        @Nullable
        private final TransformConfig config;

        // the auth state to persist after the update
        @Nullable
        private final AuthorizationState authState;

        // the action taken for the upgrade
        private final Status status;

        UpdateResult(final TransformConfig config, final AuthorizationState authState, final Status status) {
            this.config = config;
            this.authState = authState;
            this.status = status;
        }

        @Nullable
        public TransformConfig getConfig() {
            return config;
        }

        @Nullable
        public AuthorizationState getAuthState() {
            return authState;
        }

        public Status getStatus() {
            return status;
        }
    }

    /**
     * Update a single transform given a config and update
     *
     * In addition to applying update to the config, old versions of {@link TransformConfig}, {@link TransformStoredDoc} and
     * {@link TransformCheckpoint} are rewritten into the latest format and written back using {@link TransformConfigManager}
     *
     * @param securityContext the security context
     * @param indexNameExpressionResolver index name expression resolver
     * @param clusterState the current cluster state
     * @param settings settings
     * @param client a client
     * @param transformConfigManager the transform configuration manager
     * @param config the old configuration to update
     * @param update the update to apply to the configuration
     * @param seqNoPrimaryTermAndIndex sequence id and primary term of the configuration
     * @param deferValidation whether to defer some validation checks
     * @param dryRun whether to actually write the configuration back or whether to just check for updates
     * @param checkAccess whether to run access checks
     * @param hasLinkedProjects whether the current project has linked projects (skips source index privilege checks)
     * @param cloudCredentialManager UIAM credential manager; always required.
     * @param mintCloudCredential when {@code true} and UIAM is enabled, mint a new credential before writing the
     *                            config (Update); validation uses the caller-supplied credential only. When
     *                            {@code false} (Reset, Upgrade), validation loads a stored credential by
     *                            {@link TransformConfig#getCredentialId()} when no caller credential is present.
     * @param callerCredential the caller's UIAM cloud credential, extracted by the caller on the coordinating
     *                          node (e.g. from {@code UpdateTransformAction.Request#getCloudCredential()}), or
     *                          {@code null} when none is available. Validation and minting each consume their
     *                          own independent copy via {@link CloudCredential#copyOf}.
     * @param listener the listener called containing the result of the update
     */

    public static void updateTransform(
        SecurityContext securityContext,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState clusterState,
        Settings settings,
        Client client,
        TransformConfigManager transformConfigManager,
        TransformAuditor auditor,
        final TransformConfig config,
        final TransformConfigUpdate update,
        final SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        final boolean deferValidation,
        final boolean dryRun,
        final boolean checkAccess,
        final boolean hasLinkedProjects,
        final TimeValue timeout,
        final Settings destIndexSettings,
        final TransformCloudCredentialManager cloudCredentialManager,
        final boolean mintCloudCredential,
        @Nullable final CloudCredential callerCredential,
        ActionListener<UpdateResult> listener
    ) {
        // rewrite config into a new format if necessary
        final TransformConfig rewrittenConfig = TransformConfig.rewriteForUpdate(config);
        final TransformConfig updatedConfig = update != null ? update.apply(rewrittenConfig) : rewrittenConfig;
        final SetOnce<AuthorizationState> authStateHolder = new SetOnce<>();

        // <5> Update state document + checkpoints, then emit result.
        // Receives the config that was actually written so the UpdateResult carries the new credentialId.
        ActionListener<TransformConfig> updateTransformListener = listener.delegateFailureAndWrap(
            (l, persistedConfig) -> updateTransformStateAndGetLastCheckpoint(
                config.getId(),
                transformConfigManager,
                l.delegateFailureAndWrap((ll, lastCheckpoint) -> {
                    // config was updated, but the transform has no state or checkpoint
                    if (lastCheckpoint == null || lastCheckpoint == -1) {
                        ll.onResponse(new UpdateResult(persistedConfig, authStateHolder.get(), UpdateResult.Status.UPDATED));
                        return;
                    }
                    updateTransformCheckpoint(
                        config.getId(),
                        lastCheckpoint,
                        transformConfigManager,
                        ll.delegateFailureIgnoreResponseAndWrap(
                            lll -> lll.onResponse(new UpdateResult(persistedConfig, authStateHolder.get(), UpdateResult.Status.UPDATED))
                        )
                    );
                })
            )
        );

        // <4> Write the (possibly credential-stamped) config; on failure, roll back the just-minted
        // credential so we don't leak it at UIAM.
        ActionListener<Tuple<Map<String, String>, String>> writeConfigListener = listener.delegateFailureAndWrap((l, tuple) -> {
            var destIndexMappings = tuple.v1();
            var newTokenId = tuple.v2();
            var configToWrite = newTokenId == null ? updatedConfig : updatedConfig.withCredentialId(newTokenId);

            updateTransformConfiguration(
                client,
                transformConfigManager,
                auditor,
                indexNameExpressionResolver,
                configToWrite,
                destIndexMappings,
                seqNoPrimaryTermAndIndex,
                clusterState,
                destIndexSettings,
                ActionListener.wrap(r -> updateTransformListener.onResponse(configToWrite), configWriteFailure -> {
                    if (newTokenId == null || mintCloudCredential == false) {
                        // No fresh mint to roll back (Reset / Upgrade, or mint was skipped).
                        l.onFailure(configWriteFailure);
                        return;
                    }
                    logger.debug(
                        "[{}] config update failed after credential mint [{}], compensating revoke + delete",
                        updatedConfig.getId(),
                        newTokenId
                    );
                    cloudCredentialManager.loadRevokeAndDeleteByTokenId(
                        updatedConfig.getId(),
                        newTokenId,
                        ActionListener.running(() -> l.onFailure(configWriteFailure))
                    );
                })
            );
        });

        // <4> Mint cloud credential if UIAM is present. Runs after the noop/dryRun short-circuits in
        // <3> so a noop update never mints an orphan credential at UIAM. Passes callerCredential
        // directly (no copy needed): mint runs after <2> below, which already dispatched and closed
        // its own independent copy, so nothing else still needs this reference by the time mint runs.
        ActionListener<Map<String, String>> mintCredentialListener = writeConfigListener.delegateFailureAndWrap((l, destIndexMappings) -> {
            if (TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled() && mintCloudCredential) {
                cloudCredentialManager.mintAndPersist(
                    updatedConfig.getId(),
                    callerCredential,
                    l.delegateFailureAndWrap((ll, newTokenId) -> ll.onResponse(Tuple.tuple(destIndexMappings, newTokenId)))
                );
            } else {
                l.onResponse(Tuple.tuple(destIndexMappings, null));
            }
        });

        // <3> Short-circuit noop / dryRun before minting so we don't pay UIAM round-trips or leak
        // credentials for updates that won't write anything. The noop / dryRun branches respond on
        // the outer `listener` directly (they bypass mint + write entirely).
        ActionListener<Map<String, String>> validateTransformListener = mintCredentialListener.delegateFailureAndWrap(
            (l, destIndexMappings) -> {
                // If it is a noop or dry run don't write the doc
                // skip when:
                // - config is in the latest index
                // - rewrite did not change the config
                // - update is not making any changes
                if (config.getVersion() != null
                    && config.getVersion().onOrAfter(TransformInternalIndexConstants.INDEX_VERSION_LAST_CHANGED)
                    && updatedConfig.equals(config)) {
                    listener.onResponse(new UpdateResult(updatedConfig, authStateHolder.get(), UpdateResult.Status.NONE));
                    return;
                }

                if (dryRun) {
                    listener.onResponse(new UpdateResult(updatedConfig, authStateHolder.get(), UpdateResult.Status.NEEDS_UPDATE));
                    return;
                }

                l.onResponse(destIndexMappings);
            }
        );

        // <2> Validate source and destination indices
        ActionListener<AuthorizationState> checkPrivilegesListener = validateTransformListener.delegateFailureAndWrap((l, authState) -> {
            authStateHolder.set(authState);
            validateTransform(
                updatedConfig,
                client,
                deferValidation,
                timeout,
                transformConfigManager,
                cloudCredentialManager,
                mintCloudCredential,
                callerCredential,
                l
            );
        });

        // <1> Early check to verify that the user can create the destination index and can read from the source
        if (checkAccess && XPackSettings.SECURITY_ENABLED.get(settings)) {
            TransformPrivilegeChecker.checkPrivileges(
                "update",
                settings,
                securityContext,
                indexNameExpressionResolver,
                clusterState,
                client,
                updatedConfig,
                true,
                hasLinkedProjects,
                ActionListener.wrap(aVoid -> checkPrivilegesListener.onResponse(AuthorizationState.green()), e -> {
                    if (deferValidation) {
                        checkPrivilegesListener.onResponse(AuthorizationState.red(e));
                    } else {
                        checkPrivilegesListener.onFailure(e);
                    }
                })
            );
        } else { // No security enabled, just move on
            checkPrivilegesListener.onResponse(null);
        }
    }

    private static void validateTransform(
        TransformConfig config,
        Client client,
        boolean deferValidation,
        TimeValue timeout,
        TransformConfigManager transformConfigManager,
        TransformCloudCredentialManager cloudCredentialManager,
        boolean mintCloudCredential,
        @Nullable CloudCredential callerCredential,
        ActionListener<Map<String, String>> listener
    ) {
        ActionListener<ValidateTransformAction.Response> wrapped = listener.delegateFailureAndWrap(
            (l, response) -> l.onResponse(response.getDestIndexMappings())
        );

        // Update: prefer the caller-supplied UIAM credential (extracted by the caller on the
        // coordinating node; survives validate via the request payload through
        // executeAsyncWithOrigin's system-origin stash).
        //
        // Uses an independent copy: dispatchValidateTransform's receiver (TransportValidateTransformAction)
        // unconditionally closes whatever credential it's given once validate resolves (it has to, to
        // cover the redirect-to-another-node case), which would zero out callerCredential before <4>
        // in updateTransform above gets to mint with it.
        if (callerCredential != null) {
            dispatchValidateTransform(config, client, deferValidation, timeout, CloudCredential.copyOf(callerCredential), wrapped);
            return;
        }

        if (mintCloudCredential) {
            dispatchValidateTransform(config, client, deferValidation, timeout, null, wrapped);
            return;
        }

        // Reset / Upgrade: load the transform's stored internal credential when the config references one.
        var credentialId = config.getCredentialId();
        if (credentialId == null || TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled() == false) {
            dispatchValidateTransform(config, client, deferValidation, timeout, null, wrapped);
            return;
        }

        transformConfigManager.getTransformCloudCredentialByTokenId(credentialId, true, listener.delegateFailureAndWrap((l, persisted) -> {
            var storedCredential = cloudCredentialManager.cloudCredentialFromPersisted(persisted);
            dispatchValidateTransform(config, client, deferValidation, timeout, storedCredential, wrapped);
        }));
    }

    private static void dispatchValidateTransform(
        TransformConfig config,
        Client client,
        boolean deferValidation,
        TimeValue timeout,
        @Nullable CloudCredential credential,
        ActionListener<ValidateTransformAction.Response> listener
    ) {
        // Hoist into a local so we can hand the same instance to executeAsyncWithOrigin and to
        // releaseAfter, which closes the request (and its CloudCredential SecureString) once the
        // dispatch listener fires.
        var validateRequest = new ValidateTransformAction.Request(config, deferValidation, timeout, credential);
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.TRANSFORM_ORIGIN,
            ValidateTransformAction.INSTANCE,
            validateRequest,
            ActionListener.releaseAfter(listener, validateRequest)
        );
    }

    private static void updateTransformStateAndGetLastCheckpoint(
        String transformId,
        TransformConfigManager transformConfigManager,
        ActionListener<Long> listener
    ) {
        transformConfigManager.getTransformStoredDoc(transformId, true, ActionListener.wrap(currentState -> {
            if (currentState == null) {
                // no state found
                listener.onResponse(-1L);
                return;
            }

            long lastCheckpoint = currentState.v1().getTransformState().getCheckpoint();

            // if: the state is stored on the latest index, it does not need an update
            if (transformConfigManager.isLatestTransformIndex(currentState.v2().getIndex())) {
                listener.onResponse(lastCheckpoint);
                return;
            }

            // else: the state is on an old index, update by persisting it to the latest index
            transformConfigManager.putOrUpdateTransformStoredDoc(
                currentState.v1(),
                null, // set seqNoPrimaryTermAndIndex to `null` to force optype `create`, gh#80073
                ActionListener.wrap(r -> listener.onResponse(lastCheckpoint), e -> {
                    if (org.elasticsearch.ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                        // if a version conflict occurs a new state has been written between us reading and writing.
                        // this is a benign case, as it means the transform is running and the latest state has been written by it
                        logger.trace("[{}] could not update transform state during update due to running transform", transformId);
                        listener.onResponse(lastCheckpoint);
                    } else {
                        logger.warn("[{}] failed to persist transform state during update.", transformId);
                        listener.onFailure(e);
                    }
                })
            );
        }, listener::onFailure));
    }

    private static void updateTransformCheckpoint(
        String transformId,
        long lastCheckpoint,
        TransformConfigManager transformConfigManager,
        ActionListener<Boolean> listener
    ) {
        transformConfigManager.getTransformCheckpointForUpdate(transformId, lastCheckpoint, ActionListener.wrap(checkpointAndVersion -> {
            if (checkpointAndVersion == null || transformConfigManager.isLatestTransformIndex(checkpointAndVersion.v2().getIndex())) {
                listener.onResponse(true);
                return;
            }

            transformConfigManager.putTransformCheckpoint(checkpointAndVersion.v1(), listener);
        }, listener::onFailure));
    }

    private static void updateTransformConfiguration(
        Client client,
        TransformConfigManager transformConfigManager,
        TransformAuditor auditor,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransformConfig config,
        Map<String, String> destIndexMappings,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        ClusterState clusterState,
        Settings destIndexSettings,
        ActionListener<Void> listener
    ) {
        // <3> Return to the listener
        ActionListener<Boolean> putTransformConfigurationListener = ActionListener.wrap(
            putTransformConfigurationResult -> transformConfigManager.deleteOldTransformConfigurations(
                config.getId(),
                ActionListener.wrap(r -> {
                    logger.trace("[{}] successfully deleted old transform configurations", config.getId());
                    listener.onResponse(null);
                }, e -> {
                    logger.warn(LoggerMessageFormat.format("[{}] failed deleting old transform configurations.", config.getId()), e);
                    listener.onResponse(null);
                })
            ),
            // If we failed to INDEX AND we created the destination index, the destination index will still be around
            // This is a similar behavior to _start
            listener::onFailure
        );

        // <2> Update our transform
        ActionListener<Boolean> createDestinationListener = ActionListener.wrap(
            createDestResponse -> transformConfigManager.updateTransformConfiguration(
                config,
                seqNoPrimaryTermAndIndex,
                putTransformConfigurationListener
            ),
            listener::onFailure
        );

        // <1> Create destination index if necessary
        final String destinationIndex = config.getDestination().getIndex();
        String[] dest = indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), destinationIndex);

        // If we are running, we should verify that the destination index exists and create it if it does not
        if (PersistentTasksCustomMetadata.getTaskWithId(clusterState, config.getId()) != null && dest.length == 0) {
            // Resolve source indices (including remote) to verify they exist before creating the dest index.
            // The user could defer_validations and if the task is already running we allow source indices to
            // disappear. If the source and destination indices do not exist, don't do anything -- the transform
            // will just have to dynamically create the destination index without special mapping.
            resolveSourceIndicesAndCreateDestIfNeeded(
                client,
                auditor,
                indexNameExpressionResolver,
                clusterState,
                config,
                destIndexSettings,
                destIndexMappings,
                createDestinationListener
            );
        } else {
            createDestinationListener.onResponse(null);
        }
    }

    private static void resolveSourceIndicesAndCreateDestIfNeeded(
        Client client,
        TransformAuditor auditor,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState clusterState,
        TransformConfig config,
        Settings destIndexSettings,
        Map<String, String> destIndexMappings,
        ActionListener<Boolean> listener
    ) {
        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            config.getSource().getIndex(),
            config.getSource().indicesOptions()
        );
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.TRANSFORM_ORIGIN,
            ResolveIndexAction.INSTANCE,
            resolveRequest,
            ActionListener.wrap(resolveResponse -> {
                boolean hasSourceIndices = resolveResponse.getIndices().isEmpty() == false
                    || resolveResponse.getAliases().isEmpty() == false
                    || resolveResponse.getDataStreams().isEmpty() == false;
                if (hasSourceIndices) {
                    TransformIndex.createDestinationIndex(
                        client,
                        auditor,
                        indexNameExpressionResolver,
                        clusterState,
                        config,
                        destIndexSettings,
                        destIndexMappings,
                        listener
                    );
                } else {
                    listener.onResponse(null);
                }
            }, e -> {
                logger.debug(
                    () -> "[" + config.getId() + "] failed to resolve source indices during update, skipping dest index creation",
                    e
                );
                listener.onResponse(null);
            })
        );
    }

    private TransformUpdater() {}
}
