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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.CollectionUtils.concatLists;

public class SnapshotsRecoveryPlannerService implements RecoveryPlannerService {
    private final Logger logger = LogManager.getLogger(SnapshotsRecoveryPlannerService.class);

    private final ShardSnapshotsService shardSnapshotsService;
    private final boolean snapshotRecoveriesEnabled;
    private final String repository;

    public SnapshotsRecoveryPlannerService(ShardSnapshotsService shardSnapshotsService,
                                           boolean snapshotRecoveriesEnabled,
                                           String repository) {
        this.shardSnapshotsService = shardSnapshotsService;
        this.snapshotRecoveriesEnabled = snapshotRecoveriesEnabled;
        this.repository = Objects.requireNonNull(repository);
    }

    public void computeRecoveryPlan(ShardId shardId,
                                    String shardIdentifier,
                                    Store.MetadataSnapshot sourceMetadata,
                                    Store.MetadataSnapshot targetMetadata,
                                    long startingSeqNo,
                                    int translogOps,
                                    ActionListener<ShardRecoveryPlan> listener) {
        fetchAvailableSnapshotsIgnoringErrors(shardId, availableSnapshots ->
            ActionListener.completeWith(listener, () ->
                computeRecoveryPlanWithSnapshots(shardIdentifier,
                    sourceMetadata,
                    targetMetadata,
                    startingSeqNo,
                    translogOps,
                    availableSnapshots
                )
            )
        );
    }

    private ShardRecoveryPlan computeRecoveryPlanWithSnapshots(String shardIdentifier,
                                                               Store.MetadataSnapshot sourceMetadata,
                                                               Store.MetadataSnapshot targetMetadata,
                                                               long startingSeqNo,
                                                               int translogOps,
                                                               Optional<ShardSnapshot> availableSnapshot) {
        Store.RecoveryDiff sourceTargetDiff = sourceMetadata.recoveryDiff(targetMetadata);
        List<StoreFileMetadata> filesMissingInTarget = concatLists(sourceTargetDiff.missing, sourceTargetDiff.different);

        if (availableSnapshot.isEmpty()) {
            return getSourceOnlyShardRecoveryPlan(sourceMetadata, startingSeqNo, translogOps, sourceTargetDiff, filesMissingInTarget);
        }

        ShardSnapshot snapshot = availableSnapshot.get();
        // Maybe we can just rely on the diff here and remove this branch?
        if (isLogicallyEquivalentSnapshot(sourceMetadata, snapshot, shardIdentifier)) {
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

        // If we couldn't find an identical snapshot try to maximize the number of snapshot files used
        Store.MetadataSnapshot filesToRecoverFromSourceSnapshot = toMetadataSnapshot(filesMissingInTarget);
        Store.RecoveryDiff snapshotDiff = filesToRecoverFromSourceSnapshot.recoveryDiff(snapshot.getMetadataSnapshot());
        if (snapshotDiff.identical.size() > 0) {
            ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover =
                new ShardRecoveryPlan.SnapshotFilesToRecover(snapshot.getIndexId(),
                    snapshot.getRepository(),
                    snapshot.getSnapshotFiles(snapshotDiff.identical)
                );

            return new ShardRecoveryPlan(snapshotFilesToRecover,
                concatLists(snapshotDiff.missing, snapshotDiff.different),
                sourceTargetDiff.identical,
                startingSeqNo,
                translogOps,
                sourceMetadata
            );
        }

        // If we couldn't find any valid recovery plan using snapshots, fallback to the source
        return getSourceOnlyShardRecoveryPlan(sourceMetadata, startingSeqNo, translogOps, sourceTargetDiff, filesMissingInTarget);
    }

    private ShardRecoveryPlan getSourceOnlyShardRecoveryPlan(Store.MetadataSnapshot sourceMetadata,
                                                             long startingSeqNo,
                                                             int translogOps,
                                                             Store.RecoveryDiff sourceTargetDiff,
                                                             List<StoreFileMetadata> filesMissingInTarget) {
        return new ShardRecoveryPlan(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY,
            filesMissingInTarget,
            sourceTargetDiff.identical,
            startingSeqNo,
            translogOps,
            sourceMetadata
        );
    }

    private boolean isLogicallyEquivalentSnapshot(Store.MetadataSnapshot sourceMetadata,
                                                  ShardSnapshot shardSnapshot,
                                                  String sourceShardStateIdentifier) {
        if (shardSnapshot.getShardStateIdentifier() == null ||
            shardSnapshot.getShardStateIdentifier().equals(sourceShardStateIdentifier) == false) {
            return false;
        }

        Store.RecoveryDiff snapshotDiff = sourceMetadata.recoveryDiff(shardSnapshot.getMetadataSnapshot());

        // TODO: Handle primary fail-over where the snapshot is logically equivalent, but the files are different
        return snapshotDiff.different.isEmpty() == false && snapshotDiff.missing.isEmpty() == false;
    }

    private void fetchAvailableSnapshotsIgnoringErrors(ShardId shardId, Consumer<Optional<ShardSnapshot>> listener) {
        if (snapshotRecoveriesEnabled == false) {
            listener.accept(Optional.empty());
            return;
        }

        shardSnapshotsService.fetchLatestSnapshot(repository, shardId, new ActionListener<>() {
            @Override
            public void onResponse(Optional<ShardSnapshot> shardSnapshotData) {
                listener.accept(shardSnapshotData);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(new ParameterizedMessage("Unable to fetch available snapshots for shard {}", shardId), e);
                listener.accept(Optional.empty());
            }
        });
    }

    private Store.MetadataSnapshot toMetadataSnapshot(List<StoreFileMetadata> files) {
        return new Store.MetadataSnapshot(
            files
                .stream()
                .collect(Collectors.toMap(StoreFileMetadata::name, Function.identity())),
            emptyMap(),
            0
        );
    }
}
