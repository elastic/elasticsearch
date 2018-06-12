/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.OperationMode;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MaintenanceModeUpdateTask extends ClusterStateUpdateTask {
    private static final Logger logger = ESLoggerFactory.getLogger(MaintenanceModeUpdateTask.class);
    private final OperationMode mode;
    private static final Map<OperationMode, Set<OperationMode>> VALID_MODE_CHANGES = new HashMap<>();

    static {
        VALID_MODE_CHANGES.put(OperationMode.NORMAL, Sets.newHashSet(OperationMode.MAINTENANCE_REQUESTED));
        VALID_MODE_CHANGES.put(OperationMode.MAINTENANCE_REQUESTED, Sets.newHashSet(OperationMode.NORMAL, OperationMode.MAINTENANCE));
        VALID_MODE_CHANGES.put(OperationMode.MAINTENANCE, Sets.newHashSet(OperationMode.NORMAL));
    }
    public MaintenanceModeUpdateTask(OperationMode mode) {
        this.mode = mode;
    }

    OperationMode getOperationMode() {
        return mode;
    }

    @Override
    public ClusterState execute(ClusterState currentState) {
        IndexLifecycleMetadata currentMetadata = currentState.metaData().custom(IndexLifecycleMetadata.TYPE);


        if (VALID_MODE_CHANGES.get(currentMetadata.getMaintenanceMode()).contains(mode) == false) {
            return currentState;
        }

        ClusterState.Builder builder = new ClusterState.Builder(currentState);
        MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
        metadataBuilder.putCustom(IndexLifecycleMetadata.TYPE,
            new IndexLifecycleMetadata(currentMetadata.getPolicyMetadatas(), mode));
        builder.metaData(metadataBuilder.build());
        return builder.build();
    }

    @Override
    public void onFailure(String source, Exception e) {
        logger.error("unable to update lifecycle metadata with new mode [" + mode + "]", e);
    }
}
