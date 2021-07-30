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

import static java.util.Collections.emptyList;

public class UseLogicallyEquivalentSnapshotPlanner implements ShardRecoveryPlanner {

    @Override
    public ShardRecoveryPlan computePlan(String shardIdentifier,
                                         Store.MetadataSnapshot sourceMetadata,
                                         Store.MetadataSnapshot targetMetadata,
                                         long startingSeqNo,
                                         int translogOps,
                                         Store.RecoveryDiff sourceTargetDiff,
                                         List<StoreFileMetadata> filesMissingInTarget,
                                         List<ShardSnapshot> availableSnapshots) {
        for (ShardSnapshot snapshot : availableSnapshots) {
            if (snapshot.getShardStateIdentifier() == null || snapshot.getShardStateIdentifier().equals(shardIdentifier) == false) {
                continue;
            }

            Store.RecoveryDiff snapshotDiff = sourceMetadata.recoveryDiff(snapshot.getMetadataSnapshot());

            // TODO: Handle primary fail-over where the snapshot is logically equivalent, but the files are different
            if (snapshotDiff.different.isEmpty() == false || snapshotDiff.missing.isEmpty() == false) {
                continue;
            }

            ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover = new ShardRecoveryPlan.SnapshotFilesToRecover(
                snapshot.getIndexId(),
                snapshot.getRepository(),
                snapshot.getSnapshotFiles(filesMissingInTarget)
            );

            return new ShardRecoveryPlan(snapshotFilesToRecover,
                emptyList(),
                sourceTargetDiff.identical,
                startingSeqNo,
                translogOps,
                sourceMetadata
            );
        }

        return null;
    }
}
