/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.elasticsearch.common.util.CollectionUtils.concatLists;

public class RecoveryPlannerService {
    private final Logger logger = LogManager.getLogger(RecoveryPlannerService.class);

    private final ShardSnapshotsService shardSnapshotsService;
    private final ShardRecoveryPlanner shardRecoveryPlanner;
    private final BooleanSupplier snapshotRecoveriesEnabled;

    public RecoveryPlannerService(ShardSnapshotsService shardSnapshotsService,
                                  ShardRecoveryPlanner shardRecoveryPlanner,
                                  BooleanSupplier snapshotRecoveriesEnabled) {
        this.shardSnapshotsService = shardSnapshotsService;
        this.shardRecoveryPlanner = shardRecoveryPlanner;
        this.snapshotRecoveriesEnabled = snapshotRecoveriesEnabled;
    }

    public void computeRecoveryPlan(ShardId shardId,
                                    String shardIdentifier,
                                    Store.MetadataSnapshot sourceMetadata,
                                    Store.MetadataSnapshot targetMetadata,
                                    long startingSeqNo,
                                    int translogOps,
                                    ActionListener<ShardRecoveryPlan> listener) {
        fetchAvailableSnapshotsIgnoringErrors(shardId, availableSnapshots -> {
            Store.RecoveryDiff recoveryDiff = sourceMetadata.recoveryDiff(targetMetadata);
            List<StoreFileMetadata> filesMissingInTarget = concatLists(recoveryDiff.missing, recoveryDiff.different);

            ActionListener.completeWith(listener, () ->
                shardRecoveryPlanner.computePlan(shardIdentifier,
                    sourceMetadata,
                    targetMetadata,
                    startingSeqNo,
                    translogOps,
                    recoveryDiff,
                    filesMissingInTarget,
                    availableSnapshots
                )
            );
        });
    }

    void fetchAvailableSnapshotsIgnoringErrors(ShardId shardId, Consumer<List<ShardSnapshot>> listener) {
        if (snapshotRecoveriesEnabled.getAsBoolean() == false) {
            listener.accept(Collections.emptyList());
            return;
        }

        shardSnapshotsService.fetchAvailableSnapshots(shardId, new ActionListener<>() {
            @Override
            public void onResponse(List<ShardSnapshot> shardSnapshotData) {
                listener.accept(shardSnapshotData);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Unable to fetch available snapshots for shard " + shardId, e);
                listener.accept(Collections.emptyList());
            }
        });
    }
}
