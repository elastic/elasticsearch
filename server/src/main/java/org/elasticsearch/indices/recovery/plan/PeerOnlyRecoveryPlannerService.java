/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.List;

import static org.elasticsearch.common.util.CollectionUtils.concatLists;

/**
 * Service in charge of computing a {@link ShardRecoveryPlan} using only the physical files from the source peer.
 */
public class PeerOnlyRecoveryPlannerService implements RecoveryPlannerService {
    public static final RecoveryPlannerService INSTANCE = new PeerOnlyRecoveryPlannerService();

    @Override
    public void computeRecoveryPlan(
        ShardId shardId,
        @Nullable String shardStateIdentifier,
        Store.MetadataSnapshot sourceMetadata,
        Store.MetadataSnapshot targetMetadata,
        long startingSeqNo,
        int translogOps,
        IndexVersion targetVersion,
        boolean useSnapshots,
        boolean primaryRelocation,
        ActionListener<ShardRecoveryPlan> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            Store.RecoveryDiff recoveryDiff = sourceMetadata.recoveryDiff(targetMetadata);
            List<StoreFileMetadata> filesMissingInTarget = concatLists(recoveryDiff.missing, recoveryDiff.different);
            return new ShardRecoveryPlan(
                ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY,
                filesMissingInTarget,
                recoveryDiff.identical,
                startingSeqNo,
                translogOps,
                sourceMetadata
            );
        });
    }
}
