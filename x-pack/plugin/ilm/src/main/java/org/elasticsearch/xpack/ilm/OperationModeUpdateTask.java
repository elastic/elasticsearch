/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

/**
 * This task updates the operation mode state for ILM.
 *
 * As stopping ILM proved to be an action we want to sometimes take in order to allow clusters to stabilise when under heavy load this
 * task might run at {@link Priority#IMMEDIATE} priority so please make sure to keep this task as lightweight as possible.
 */
public class OperationModeUpdateTask extends ClusterStateUpdateTask {
    private static final Logger logger = LogManager.getLogger(OperationModeUpdateTask.class);
    @Nullable
    private final OperationMode ilmMode;
    @Nullable
    private final OperationMode slmMode;

    private OperationModeUpdateTask(Priority priority, OperationMode ilmMode, OperationMode slmMode) {
        super(priority);
        this.ilmMode = ilmMode;
        this.slmMode = slmMode;
    }

    public static OperationModeUpdateTask ilmMode(OperationMode mode) {
        return ilmMode(Priority.NORMAL, mode);
    }

    public static OperationModeUpdateTask ilmMode(Priority priority, OperationMode mode) {
        return new OperationModeUpdateTask(priority, mode, null);
    }

    public static OperationModeUpdateTask slmMode(OperationMode mode) {
        return new OperationModeUpdateTask(Priority.NORMAL, null, mode);
    }

    OperationMode getILMOperationMode() {
        return ilmMode;
    }

    OperationMode getSLMOperationMode() {
        return slmMode;
    }

    @Override
    public ClusterState execute(ClusterState currentState) {
        ClusterState newState = currentState;
        newState = updateILMState(newState);
        newState = updateSLMState(newState);
        return newState;
    }

    private ClusterState updateILMState(final ClusterState currentState) {
        if (ilmMode == null) {
            return currentState;
        }
        IndexLifecycleMetadata currentMetadata = currentState.metadata().custom(IndexLifecycleMetadata.TYPE);
        if (currentMetadata != null && currentMetadata.getOperationMode().isValidChange(ilmMode) == false) {
            return currentState;
        } else if (currentMetadata == null) {
            currentMetadata = IndexLifecycleMetadata.EMPTY;
        }

        final OperationMode newMode;
        if (currentMetadata.getOperationMode().isValidChange(ilmMode)) {
            newMode = ilmMode;
        } else {
            newMode = currentMetadata.getOperationMode();
        }

        if (newMode.equals(ilmMode) == false) {
            logger.info("updating ILM operation mode to {}", newMode);
        }
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata())
                    .putCustom(IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(currentMetadata.getPolicyMetadatas(), newMode)))
            .build();
    }

    private ClusterState updateSLMState(final ClusterState currentState) {
        if (slmMode == null) {
            return currentState;
        }
        SnapshotLifecycleMetadata currentMetadata = currentState.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        if (currentMetadata != null && currentMetadata.getOperationMode().isValidChange(slmMode) == false) {
            return currentState;
        } else if (currentMetadata == null) {
            currentMetadata = SnapshotLifecycleMetadata.EMPTY;
        }

        final OperationMode newMode;
        if (currentMetadata.getOperationMode().isValidChange(slmMode)) {
            newMode = slmMode;
        } else {
            newMode = currentMetadata.getOperationMode();
        }

        if (newMode.equals(slmMode) == false) {
            logger.info("updating SLM operation mode to {}", newMode);
        }
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata())
                .putCustom(SnapshotLifecycleMetadata.TYPE,
                    new SnapshotLifecycleMetadata(currentMetadata.getSnapshotConfigurations(),
                        newMode, currentMetadata.getStats())))
            .build();
    }

    @Override
    public void onFailure(String source, Exception e) {
        logger.error("unable to update lifecycle metadata with new ilm mode [" + ilmMode + "], slm mode [" + slmMode + "]", e);
    }
}
