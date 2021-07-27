/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

class ShardRecoveryPlanner {
    private final Logger logger = LogManager.getLogger(ShardRecoveryPlanner.class);

    private final ShardId shardId;
    private final String shardIdentifier;
    private final Store.MetadataSnapshot recoverySourceMetadata;
    private final long startingSeqNo;
    private final int translogOps;

    private final Store.RecoveryDiff sourceTargetDiff;
    private final ShardSnapshotsService shardSnapshotsService;

    private final boolean snapshotRecoveriesEnabled;

    ShardRecoveryPlanner(ShardId shardId,
                         @Nullable String shardIdentifier,
                         Store.MetadataSnapshot sourceMetadataSnapshot,
                         Store.MetadataSnapshot targetMetadataSnapshot,
                         long startingSeqNo,
                         int translogOps,
                         ShardSnapshotsService shardSnapshotsService,
                         boolean snapshotRecoveriesEnabled) {
        this.shardId = shardId;
        this.shardIdentifier = shardIdentifier;
        this.recoverySourceMetadata = sourceMetadataSnapshot;
        this.startingSeqNo = startingSeqNo;
        this.translogOps = translogOps;

        // Generate a "diff" of all the identical, different, and missing
        // segment files on the target node, using the existing files on
        // the source node
        this.sourceTargetDiff = sourceMetadataSnapshot.recoveryDiff(targetMetadataSnapshot);

        this.shardSnapshotsService = shardSnapshotsService;
        this.snapshotRecoveriesEnabled = snapshotRecoveriesEnabled;
    }

    public void computeRecoveryPlan(ActionListener<ShardRecoveryPlan> listener) {
        if (snapshotRecoveriesEnabled) {
            computePlanWithSnapshots(listener);
        } else {
            computePlanWithoutSnapshots(listener);
        }
    }

    private void computePlanWithoutSnapshots(ActionListener<ShardRecoveryPlan> listener) {
        final ShardRecoveryPlan shardRecoveryPlan = getShardRecoveryPlanWithoutUsingSnapshots();
        listener.onResponse(shardRecoveryPlan);
    }

    private ShardRecoveryPlan getShardRecoveryPlanWithoutUsingSnapshots() {
        return buildRecoveryPlan(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY, sourceTargetDiff.missingAndDifferent);
    }

    private void computePlanWithSnapshots(ActionListener<ShardRecoveryPlan> listener) {
        shardSnapshotsService.fetchAvailableSnapshots(shardId, new ActionListener<>() {
            @Override
            public void onResponse(List<ShardSnapshotsService.ShardSnapshotData> availableSnapshots) {
                planUsingAvailableSnapshots(availableSnapshots, listener);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Unable to fetch available snapshots for shard " + shardId, e);
                computePlanWithoutSnapshots(listener);
            }
        });
    }

    private void planUsingAvailableSnapshots(List<ShardSnapshotsService.ShardSnapshotData> availableSnapshots,
                                             ActionListener<ShardRecoveryPlan> listener) {
        ActionListener.completeWith(listener, () -> {
            if (availableSnapshots.isEmpty()) {
                return getShardRecoveryPlanWithoutUsingSnapshots();
            }

            ShardRecoveryPlan plan = useLogicallyEquivalentSnapshotIfAvailable(availableSnapshots);

            if (plan != null) {
                return plan;
            }

            // TODO: Check if it's possible to use a snapshot + translog replay

            plan = useMostFilesFromAvailableSnapshots(availableSnapshots);

            // If we weren't able to find any suitable snapshot, send all missing and different files from the source node
            if (plan == null) {
                plan = getShardRecoveryPlanWithoutUsingSnapshots();
            }

            return plan;
        });
    }

    @Nullable
    private ShardRecoveryPlan useLogicallyEquivalentSnapshotIfAvailable(List<ShardSnapshotsService.ShardSnapshotData> availableSnapshots) {
        List<ShardSnapshotsService.ShardSnapshotData> equivalentSnapshots = availableSnapshots.stream()
            .filter(snapshot -> snapshot.getShardStateIdentifier() != null)
            .filter(snapshot -> snapshot.getShardStateIdentifier().equals(shardIdentifier))
            .collect(Collectors.toList());

        for (ShardSnapshotsService.ShardSnapshotData equivalentSnapshot : equivalentSnapshots) {
            final Store.RecoveryDiff snapshotDiff = recoverySourceMetadata.recoveryDiff(equivalentSnapshot.getMetadataSnapshot());

            if (snapshotDiff.different.isEmpty() == false || snapshotDiff.missing.isEmpty() == false) {
                // TODO: Handle primary fail-over
                continue;
            }

            ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover = new ShardRecoveryPlan.SnapshotFilesToRecover(
                equivalentSnapshot.getIndexId(),
                equivalentSnapshot.getSnapshotFiles(sourceTargetDiff.missingAndDifferent)
            );

            return buildRecoveryPlan(snapshotFilesToRecover, emptyList());
        }

        return null;
    }

    private ShardRecoveryPlan useMostFilesFromAvailableSnapshots(List<ShardSnapshotsService.ShardSnapshotData> availableSnapshots) {
        Map<String, StoreFileMetadata> filesToRecoverFromSource = sourceTargetDiff.missingAndDifferent
            .stream()
            .collect(Collectors.toMap(StoreFileMetadata::name, Function.identity()));
        Store.MetadataSnapshot filesToRecoverFromSourceSnapshot =
            new Store.MetadataSnapshot(filesToRecoverFromSource, emptyMap(), 0);

        int filesToRecoverFromSnapshot = 0;
        ShardRecoveryPlan plan = null;
        for (ShardSnapshotsService.ShardSnapshotData shardSnapshot : availableSnapshots) {
            final Store.RecoveryDiff snapshotDiff = filesToRecoverFromSourceSnapshot.recoveryDiff(shardSnapshot.getMetadataSnapshot());
            if (snapshotDiff.identical.size() > filesToRecoverFromSnapshot) {
                final ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover =
                    new ShardRecoveryPlan.SnapshotFilesToRecover(shardSnapshot.getIndexId(),
                        shardSnapshot.getSnapshotFiles(snapshotDiff.identical));

                plan = buildRecoveryPlan(snapshotFilesToRecover, snapshotDiff.missingAndDifferent);
                filesToRecoverFromSnapshot = plan.getSnapshotFilesToRecover().getFiles().size();
            }
        }

        return plan;
    }

    private ShardRecoveryPlan buildRecoveryPlan(ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover,
                                                List<StoreFileMetadata> sourceFilesToRecover) {
        return new ShardRecoveryPlan(snapshotFilesToRecover,
            sourceFilesToRecover,
            sourceTargetDiff.identical,
            startingSeqNo,
            translogOps,
            recoverySourceMetadata
        );
    }
}
