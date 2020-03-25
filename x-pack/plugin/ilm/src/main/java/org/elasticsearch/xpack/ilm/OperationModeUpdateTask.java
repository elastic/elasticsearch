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
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

public class OperationModeUpdateTask extends ClusterStateUpdateTask {
    private static final Logger logger = LogManager.getLogger(OperationModeUpdateTask.class);
    @Nullable
    private final OperationMode ilmMode;
    @Nullable
    private final OperationMode slmMode;

    private OperationModeUpdateTask(OperationMode ilmMode, OperationMode slmMode) {
        this.ilmMode = ilmMode;
        this.slmMode = slmMode;
    }

    public static OperationModeUpdateTask ilmMode(OperationMode mode) {
        return new OperationModeUpdateTask(mode, null);
    }

    public static OperationModeUpdateTask slmMode(OperationMode mode) {
        return new OperationModeUpdateTask(null, mode);
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
        IndexLifecycleMetadata currentMetadata = currentState.metaData().custom(IndexLifecycleMetadata.TYPE);
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
            .metaData(MetaData.builder(currentState.metaData())
                    .putCustom(IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(currentMetadata.getPolicyMetadatas(), newMode)))
            .build();
    }

    private ClusterState updateSLMState(final ClusterState currentState) {
        if (slmMode == null) {
            return currentState;
        }
        SnapshotLifecycleMetadata currentMetadata = currentState.metaData().custom(SnapshotLifecycleMetadata.TYPE);
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
            .metaData(MetaData.builder(currentState.metaData())
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
