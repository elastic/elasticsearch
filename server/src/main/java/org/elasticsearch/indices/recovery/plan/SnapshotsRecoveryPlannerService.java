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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.util.Collections;
import java.util.List;
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
                                           @Nullable String repository) {
        this.shardSnapshotsService = shardSnapshotsService;
        this.snapshotRecoveriesEnabled = snapshotRecoveriesEnabled;
        this.repository = repository;
    }

    public void computeRecoveryPlan(ShardId shardId,
                                    String shardIdentifier,
                                    Store.MetadataSnapshot sourceMetadata,
                                    Store.MetadataSnapshot targetMetadata,
                                    long startingSeqNo,
                                    int translogOps,
                                    Version targetVersion,
                                    ActionListener<ShardRecoveryPlan> listener) {
        // Fallback to source only recovery if the target node is in an incompatible version
        if (targetVersion.onOrAfter(RecoverySettings.SNAPSHOT_RECOVERIES_SUPPORTED_VERSION) == false) {
            Store.RecoveryDiff sourceTargetDiff = sourceMetadata.recoveryDiff(targetMetadata);
            listener.onResponse(
                new ShardRecoveryPlan(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY,
                    concatLists(sourceTargetDiff.missing, sourceTargetDiff.different),
                    sourceTargetDiff.identical,
                    startingSeqNo,
                    translogOps,
                    sourceMetadata
                )
            );
            return;
        }

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
                                                               List<ShardSnapshot> availableSnapshots) {
        Store.RecoveryDiff sourceTargetDiff = sourceMetadata.recoveryDiff(targetMetadata);
        List<StoreFileMetadata> filesMissingInTarget = concatLists(sourceTargetDiff.missing, sourceTargetDiff.different);

        Store.MetadataSnapshot filesToRecoverFromSourceSnapshot = toMetadataSnapshot(filesMissingInTarget);

        ShardRecoveryPlan plan = null;
        int filesToRecoverFromSnapshot = 0;
        for (ShardSnapshot snapshot : availableSnapshots) {
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
            Store.RecoveryDiff snapshotDiff = filesToRecoverFromSourceSnapshot.recoveryDiff(snapshot.getMetadataSnapshot());
            if (snapshotDiff.identical.size() > filesToRecoverFromSnapshot) {
                ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover =
                    new ShardRecoveryPlan.SnapshotFilesToRecover(snapshot.getIndexId(),
                        snapshot.getRepository(),
                        snapshot.getSnapshotFiles(snapshotDiff.identical));

                plan = new ShardRecoveryPlan(snapshotFilesToRecover,
                    concatLists(snapshotDiff.missing, snapshotDiff.different),
                    sourceTargetDiff.identical,
                    startingSeqNo,
                    translogOps,
                    sourceMetadata
                );
                filesToRecoverFromSnapshot = plan.getSnapshotFilesToRecover().size();
            }
        }

        if (plan != null) {
            return plan;
        }

        // If we couldn't find any valid recovery plan using snapshots, fallback to the source
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

    private void fetchAvailableSnapshotsIgnoringErrors(ShardId shardId, Consumer<List<ShardSnapshot>> listener) {
        if (snapshotRecoveriesEnabled == false) {
            listener.accept(Collections.emptyList());
            return;
        }

        ActionListener<List<ShardSnapshot>> listenerIgnoringErrors = new ActionListener<>() {
            @Override
            public void onResponse(List<ShardSnapshot> shardSnapshotData) {
                listener.accept(shardSnapshotData);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(new ParameterizedMessage("Unable to fetch available snapshots for shard {}", shardId), e);
                listener.accept(Collections.emptyList());
            }
        };

        if (Strings.isNullOrEmpty(repository) == false) {
            shardSnapshotsService.fetchAvailableSnapshots(repository, shardId, listenerIgnoringErrors);
        } else {
            shardSnapshotsService.fetchAvailableSnapshotsInAllRepositories(shardId, listenerIgnoringErrors);
        }
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
