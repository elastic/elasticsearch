/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo.TransformCheckpointingInfoBuilder;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

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

    private final TransformConfigManager transformConfigManager;
    private final TransformAuditor transformAuditor;
    private final RemoteClusterResolver remoteClusterResolver;

    public TransformCheckpointService(
        final Settings settings,
        final ClusterService clusterService,
        final TransformConfigManager transformConfigManager,
        TransformAuditor transformAuditor
    ) {
        this.transformConfigManager = transformConfigManager;
        this.transformAuditor = transformAuditor;
        this.remoteClusterResolver = new RemoteClusterResolver(settings, clusterService.getClusterSettings());
    }

    public CheckpointProvider getCheckpointProvider(final Client client, final TransformConfig transformConfig) {
        if (transformConfig.getSyncConfig() instanceof TimeSyncConfig) {
            return new TimeBasedCheckpointProvider(
                client,
                remoteClusterResolver,
                transformConfigManager,
                transformAuditor,
                transformConfig
            );
        }

        return new DefaultCheckpointProvider(client, remoteClusterResolver, transformConfigManager, transformAuditor, transformConfig);
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
        final Client client,
        final String transformId,
        final long lastCheckpointNumber,
        final TransformIndexerPosition nextCheckpointPosition,
        final TransformProgress nextCheckpointProgress,
        final ActionListener<TransformCheckpointingInfoBuilder> listener
    ) {

        // we need to retrieve the config first before we can defer the rest to the corresponding provider
        transformConfigManager.getTransformConfiguration(transformId, ActionListener.wrap(transformConfig -> {
            getCheckpointProvider(client, transformConfig).getCheckpointingInfo(
                lastCheckpointNumber,
                nextCheckpointPosition,
                nextCheckpointProgress,
                listener
            );
        }, transformError -> {
            logger.warn("Failed to retrieve configuration for transform [" + transformId + "]", transformError);
            listener.onFailure(new CheckpointException("Failed to retrieve configuration", transformError));
        }));
    }
}
