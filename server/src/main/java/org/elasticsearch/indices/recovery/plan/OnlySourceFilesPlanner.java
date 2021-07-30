/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.List;

public class OnlySourceFilesPlanner implements ShardRecoveryPlanner {
    @Override
    public ShardRecoveryPlan computePlan(String shardIdentifier,
                                         Store.MetadataSnapshot sourceMetadata,
                                         Store.MetadataSnapshot targetMetadata,
                                         long startingSeqNo,
                                         int translogOps,
                                         Store.RecoveryDiff sourceTargetDiff,
                                         List<StoreFileMetadata> filesMissingInTarget,
                                         List<ShardSnapshot> availableSnapshots) {
        return new ShardRecoveryPlan(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY,
            filesMissingInTarget,
            sourceTargetDiff.identical,
            startingSeqNo,
            translogOps,
            targetMetadata
        );
    }
}
