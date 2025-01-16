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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.TransformDestIndexSettings;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformIndex;

import java.time.Clock;
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
        private final TransformConfig config;

        // the action taken for the upgrade
        private final Status status;

        UpdateResult(final TransformConfig config, final Status status) {
            this.config = config;
            this.status = status;
        }

        public Status getStatus() {
            return status;
        }

        @Nullable
        public TransformConfig getConfig() {
            return config;
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
     * @param listener the listener called containing the result of the update
     */

    public static void updateTransform(
        XPackLicenseState licenseState,
        SecurityContext securityContext,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState clusterState,
        Settings settings,
        Client client,
        TransformConfigManager transformConfigManager,
        final TransformConfig config,
        final TransformConfigUpdate update,
        final SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        final boolean deferValidation,
        final boolean dryRun,
        final boolean checkAccess,
        final TimeValue timeout,
        ActionListener<UpdateResult> listener
    ) {
        // rewrite config into a new format if necessary
        TransformConfig rewrittenConfig = TransformConfig.rewriteForUpdate(config);
        TransformConfig updatedConfig = update != null ? update.apply(rewrittenConfig) : rewrittenConfig;

        // <5> Update checkpoints
        ActionListener<Long> updateStateListener = ActionListener.wrap(lastCheckpoint -> {
            // config was updated, but the transform has no state or checkpoint
            if (lastCheckpoint == null || lastCheckpoint == -1) {
                listener.onResponse(new UpdateResult(updatedConfig, UpdateResult.Status.UPDATED));
                return;
            }

            updateTransformCheckpoint(
                config.getId(),
                lastCheckpoint,
                transformConfigManager,
                ActionListener.wrap(
                    r -> listener.onResponse(new UpdateResult(updatedConfig, UpdateResult.Status.UPDATED)),
                    listener::onFailure
                )
            );
        }, listener::onFailure);

        // <4> Update State document
        ActionListener<Void> updateTransformListener = ActionListener.wrap(
            r -> updateTransformStateAndGetLastCheckpoint(config.getId(), transformConfigManager, updateStateListener),
            listener::onFailure
        );

        // <3> Update the transform
        ActionListener<Map<String, String>> validateTransformListener = ActionListener.wrap(destIndexMappings -> {
            // If it is a noop or dry run don't write the doc
            // skip when:
            // - config is in the latest index
            // - rewrite did not change the config
            // - update is not making any changes
            if (config.getVersion() != null
                && config.getVersion().onOrAfter(TransformInternalIndexConstants.INDEX_VERSION_LAST_CHANGED)
                && updatedConfig.equals(config)) {
                listener.onResponse(new UpdateResult(updatedConfig, UpdateResult.Status.NONE));
                return;
            }

            if (dryRun) {
                listener.onResponse(new UpdateResult(updatedConfig, UpdateResult.Status.NEEDS_UPDATE));
                return;
            }

            updateTransformConfiguration(
                client,
                transformConfigManager,
                indexNameExpressionResolver,
                updatedConfig,
                destIndexMappings,
                seqNoPrimaryTermAndIndex,
                clusterState,
                ActionListener.wrap(r -> updateTransformListener.onResponse(null), listener::onFailure)
            );
        }, listener::onFailure);

        // <2> Validate source and destination indices
        ActionListener<Void> checkPrivilegesListener = ActionListener.wrap(aVoid -> {
            validateTransform(updatedConfig, client, deferValidation, timeout, validateTransformListener);
        }, listener::onFailure);

        // <1> Early check to verify that the user can create the destination index and can read from the source
        if (checkAccess && licenseState.isSecurityEnabled() && deferValidation == false) {
            TransformPrivilegeChecker.checkPrivileges(
                "update",
                securityContext,
                indexNameExpressionResolver,
                clusterState,
                client,
                updatedConfig,
                true,
                checkPrivilegesListener
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
        ActionListener<Map<String, String>> listener
    ) {
        client.execute(
            ValidateTransformAction.INSTANCE,
            new ValidateTransformAction.Request(config, deferValidation, timeout),
            ActionListener.wrap(response -> listener.onResponse(response.getDestIndexMappings()), listener::onFailure)
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
            if (currentState.v2().getIndex().equals(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME)) {
                listener.onResponse(lastCheckpoint);
                return;
            }

            // else: the state is on an old index, update by persisting it to the latest index
            transformConfigManager.putOrUpdateTransformStoredDoc(
                currentState.v1(),
                null, // set seqNoPrimaryTermAndIndex to `null` to force optype `create`, gh#80073
                ActionListener.wrap(r -> { listener.onResponse(lastCheckpoint); }, e -> {
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
            if (checkpointAndVersion == null
                || checkpointAndVersion.v2().getIndex().equals(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME)) {
                listener.onResponse(true);
                return;
            }

            transformConfigManager.putTransformCheckpoint(checkpointAndVersion.v1(), listener);
        }, listener::onFailure));
    }

    private static void updateTransformConfiguration(
        Client client,
        TransformConfigManager transformConfigManager,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransformConfig config,
        Map<String, String> mappings,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        ClusterState clusterState,
        ActionListener<Void> listener
    ) {
        // <3> Return to the listener
        ActionListener<Boolean> putTransformConfigurationListener = ActionListener.wrap(putTransformConfigurationResult -> {
            transformConfigManager.deleteOldTransformConfigurations(config.getId(), ActionListener.wrap(r -> {
                logger.trace("[{}] successfully deleted old transform configurations", config.getId());
                listener.onResponse(null);
            }, e -> {
                logger.warn(LoggerMessageFormat.format("[{}] failed deleting old transform configurations.", config.getId()), e);
                listener.onResponse(null);
            }));
        },
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
        String[] dest = indexNameExpressionResolver.concreteIndexNames(
            clusterState,
            IndicesOptions.lenientExpandOpen(),
            config.getDestination().getIndex()
        );
        String[] src = indexNameExpressionResolver.concreteIndexNames(
            clusterState,
            IndicesOptions.lenientExpandOpen(),
            true,
            config.getSource().getIndex()
        );
        // If we are running, we should verify that the destination index exists and create it if it does not
        if (PersistentTasksCustomMetadata.getTaskWithId(clusterState, config.getId()) != null && dest.length == 0
        // Verify we have source indices. The user could defer_validations and if the task is already running
        // we allow source indices to disappear. If the source and destination indices do not exist, don't do anything
        // the transform will just have to dynamically create the destination index without special mapping.
            && src.length > 0) {
            createDestinationIndex(client, config, mappings, createDestinationListener);
        } else {
            createDestinationListener.onResponse(null);
        }
    }

    private static void createDestinationIndex(
        Client client,
        TransformConfig config,
        Map<String, String> mappings,
        ActionListener<Boolean> listener
    ) {
        TransformDestIndexSettings generatedDestIndexSettings = TransformIndex.createTransformDestIndexSettings(
            mappings,
            config.getId(),
            Clock.systemUTC()
        );
        TransformIndex.createDestinationIndex(client, config, generatedDestIndexSettings, listener);
    }

    private TransformUpdater() {}

}
