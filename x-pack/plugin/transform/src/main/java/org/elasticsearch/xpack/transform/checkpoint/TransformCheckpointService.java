/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo.TransformCheckpointingInfoBuilder;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.transform.action.TransformCloudCredentialManager;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

import java.time.Clock;
import java.util.function.Supplier;

/**
 * Transform Checkpoint Service
 *
 * Allows checkpointing a source of a transform which includes all relevant checkpoints of the source.
 *
 * This will be used to checkpoint a transform, detect changes, run the transform in continuous mode.
 *
 */
public class TransformCheckpointService {

    private static final Logger logger = LogManager.getLogger(TransformCheckpointService.class);

    private final Clock clock;
    private final TransformConfigManager transformConfigManager;
    private final TransformAuditor transformAuditor;
    private final CrossProjectModeDecider crossProjectModeDecider;
    private final TransformCloudCredentialManager cloudCredentialManager;

    public TransformCheckpointService(
        final Clock clock,
        final TransformConfigManager transformConfigManager,
        TransformAuditor transformAuditor,
        CrossProjectModeDecider crossProjectModeDecider,
        TransformCloudCredentialManager cloudCredentialManager
    ) {
        this.clock = clock;
        this.transformConfigManager = transformConfigManager;
        this.transformAuditor = transformAuditor;
        this.crossProjectModeDecider = crossProjectModeDecider;
        this.cloudCredentialManager = cloudCredentialManager;
    }

    public CheckpointProvider getCheckpointProvider(
        final ParentTaskAssigningClient client,
        final TransformConfig transformConfig,
        final Supplier<PersistedCloudCredential> credentialSupplier
    ) {
        Supplier<Client> clientSupplier = () -> cloudCredentialManager.wrapWithPersistedIfPresent(client, credentialSupplier.get());
        Supplier<ThreadContext> threadContextSupplier = () -> client.threadPool().getThreadContext();
        if (transformConfig.getSyncConfig() instanceof TimeSyncConfig) {
            return new TimeBasedCheckpointProvider(
                clock,
                threadContextSupplier,
                clientSupplier,
                transformConfigManager,
                transformAuditor,
                transformConfig,
                crossProjectModeDecider
            );
        }

        return new DefaultCheckpointProvider(
            clock,
            threadContextSupplier,
            clientSupplier,
            transformConfigManager,
            transformAuditor,
            transformConfig,
            crossProjectModeDecider
        );
    }

    /**
     * Get checkpointing stats for a stopped transform
     *
     * @param transformId The transform id
     * @param lastCheckpointNumber the last checkpoint
     * @param nextCheckpointPosition position for the next checkpoint
     * @param nextCheckpointProgress progress for the next checkpoint
     * @param listener listener to retrieve the result
     */
    public void getCheckpointingInfo(
        final ParentTaskAssigningClient client,
        final TimeValue timeout,
        final String transformId,
        final long lastCheckpointNumber,
        final TransformIndexerPosition nextCheckpointPosition,
        final TransformProgress nextCheckpointProgress,
        final ActionListener<TransformCheckpointingInfoBuilder> listener
    ) {

        // we need to retrieve the config first before we can defer the rest to the corresponding provider
        transformConfigManager.getTransformConfiguration(transformId, ActionListener.wrap(transformConfig -> {
            var credentialId = transformConfig.getCredentialId();
            if (credentialId != null && TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled()) {
                transformConfigManager.getTransformCloudCredentialByTokenId(
                    credentialId,
                    true,
                    listener.delegateFailureAndWrap((l, persisted) -> {
                        getCheckpointProvider(client, transformConfig, () -> persisted).getCheckpointingInfo(
                            lastCheckpointNumber,
                            nextCheckpointPosition,
                            nextCheckpointProgress,
                            timeout,
                            ActionListener.releaseAfter(l, persisted)
                        );
                    })
                );
            } else {
                getCheckpointProvider(client, transformConfig, () -> null).getCheckpointingInfo(
                    lastCheckpointNumber,
                    nextCheckpointPosition,
                    nextCheckpointProgress,
                    timeout,
                    listener
                );
            }
        }, transformError -> {
            logger.warn("Failed to retrieve configuration for transform [" + transformId + "]", transformError);
            listener.onFailure(new CheckpointException("Failed to retrieve configuration", transformError));
        }));
    }

    /**
     * Derives basic checkpointing stats for a stopped transform.  This does not make a call to obtain any additional information.
     * This will only read checkpointing information from the TransformState.
     *
     * @param transformState the current state of the Transform
     * @return basic checkpointing info, including id, position, and progress of the Next Checkpoint and the id of the Last Checkpoint.
     */
    public static TransformCheckpointingInfo deriveBasicCheckpointingInfo(TransformState transformState) {
        return new TransformCheckpointingInfo(lastCheckpointStats(transformState), nextCheckpointStats(transformState), 0L, null, null);
    }

    private static TransformCheckpointStats lastCheckpointStats(TransformState transformState) {
        return new TransformCheckpointStats(transformState.getCheckpoint(), null, null, 0L, 0L);
    }

    private static TransformCheckpointStats nextCheckpointStats(TransformState transformState) {
        // getCheckpoint is the last checkpoint. if we're at zero then we'd only call to get the zeroth checkpoint (see getCheckpointInfo)
        var checkpoint = transformState.getCheckpoint() != 0 ? transformState.getCheckpoint() + 1 : 0;
        return new TransformCheckpointStats(checkpoint, transformState.getPosition(), transformState.getProgress(), 0L, 0L);
    }
}
