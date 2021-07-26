/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

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

public class ShardRecoveryIndexPlanner {
    private final ShardId shardId;
    private final String shardIdentifier;
    private final Store.MetadataSnapshot recoverySourceMetadata;
    private final long startingSeqNo;
    private final int translogOps;

    private final Store.RecoveryDiff targetDiff;
    private final SnapshotInfoFetcher snapshotInfoFetcher;

    private final boolean snapshotRecoveriesEnabled;

    public ShardRecoveryIndexPlanner(ShardId shardId,
                                     String shardIdentifier,
                                     Store.MetadataSnapshot recoverySourceMetadata,
                                     Store.MetadataSnapshot recoveryTargetMetadata,
                                     long startingSeqNo,
                                     int translogOps,
                                     SnapshotInfoFetcher snapshotInfoFetcher,
                                     boolean snapshotRecoveriesEnabled) {
        this.shardId = shardId;
        this.shardIdentifier = shardIdentifier;
        this.recoverySourceMetadata = recoverySourceMetadata;
        this.startingSeqNo = startingSeqNo;
        this.translogOps = translogOps;

        // Generate a "diff" of all the identical, different, and missing
        // segment files on the target node, using the existing files on
        // the source node
        this.targetDiff =  recoverySourceMetadata.recoveryDiff(recoveryTargetMetadata);

        this.snapshotInfoFetcher = snapshotInfoFetcher;
        this.snapshotRecoveriesEnabled = snapshotRecoveriesEnabled;
    }

    public void computeRecoveryPlan(ActionListener<ShardRecoveryPlan> listener) {
        if (snapshotRecoveriesEnabled == false) {
            computePlanWithoutUsingSnapshots(listener);
        } else {
            computePlanUsingSnapshots(listener);
        }
    }

    private void computePlanWithoutUsingSnapshots(ActionListener<ShardRecoveryPlan> listener) {
        final ShardRecoveryPlan shardRecoveryPlan = getShardRecoveryPlanWithoutUsingSnapshots();
        listener.onResponse(shardRecoveryPlan);
    }

    private ShardRecoveryPlan getShardRecoveryPlanWithoutUsingSnapshots() {
        return buildRecoveryPlan(ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY, targetDiff.missingAndDifferent);
    }

    private void computePlanUsingSnapshots(ActionListener<ShardRecoveryPlan> listener) {
        snapshotInfoFetcher.fetchAvailableSnapshots(shardId, new ActionListener<>() {
            @Override
            public void onResponse(List<SnapshotInfoFetcher.ShardSnapshotData> availableSnapshots) {
                planUsingAvailableSnapshots(availableSnapshots, listener);
            }

            @Override
            public void onFailure(Exception e) {
                // TODO: log error
                computePlanWithoutUsingSnapshots(listener);
            }
        });
    }

    private void planUsingAvailableSnapshots(List<SnapshotInfoFetcher.ShardSnapshotData> availableSnapshots,
                                             ActionListener<ShardRecoveryPlan> listener) {
        ShardRecoveryPlan plan = useLogicallyEquivalentSnapshotIfAvailable(availableSnapshots);

        if (plan != null) {
            listener.onResponse(plan);
            return;
        }

        // TODO: Check if it's possible to use a snapshot + translog replay

        plan = useMostFilesFromAvailableSnapshots(availableSnapshots);
        listener.onResponse(plan);
    }

    @Nullable
    private ShardRecoveryPlan useLogicallyEquivalentSnapshotIfAvailable(List<SnapshotInfoFetcher.ShardSnapshotData> availableSnapshots) {
        List<SnapshotInfoFetcher.ShardSnapshotData> equivalentSnapshots = availableSnapshots.stream()
            .filter(snapshot -> snapshot.getShardStateIdentifier() != null)
            .filter(snapshot -> snapshot.getShardStateIdentifier().equals(shardIdentifier))
            .collect(Collectors.toList());

        for (SnapshotInfoFetcher.ShardSnapshotData equivalentSnapshot : equivalentSnapshots) {
            final Store.RecoveryDiff snapshotDiff = recoverySourceMetadata.recoveryDiff(equivalentSnapshot.getMetadataSnapshot());

            if (snapshotDiff.different.isEmpty() == false || snapshotDiff.missing.isEmpty() == false) {
                // TODO: Handle primary fail-over
                continue;
            }

            final ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover =
                new ShardRecoveryPlan.SnapshotFilesToRecover(equivalentSnapshot.getIndexId(),
                    equivalentSnapshot.getSnapshotFiles(snapshotDiff.identical));
            return new ShardRecoveryPlan(snapshotFilesToRecover, emptyList(), targetDiff.identical, startingSeqNo, translogOps,
                recoverySourceMetadata);
        }

        return null;
    }

    private ShardRecoveryPlan useMostFilesFromAvailableSnapshots(List<SnapshotInfoFetcher.ShardSnapshotData> availableSnapshots) {
        ShardRecoveryPlan plan = getShardRecoveryPlanWithoutUsingSnapshots();

        Map<String, StoreFileMetadata> filesToRecoverFromSource = plan.getSourceFilesToRecover()
            .stream()
            .collect(Collectors.toMap(StoreFileMetadata::name, Function.identity()));
        Store.MetadataSnapshot filesToRecoverFromSourceSnapshot =
            new Store.MetadataSnapshot(filesToRecoverFromSource, emptyMap(), 0);

        int filesToRecoverFromSnapshot = 0;
        for (SnapshotInfoFetcher.ShardSnapshotData shardSnapshot : availableSnapshots) {
            final Store.RecoveryDiff snapshotDiff = filesToRecoverFromSourceSnapshot.recoveryDiff(shardSnapshot.getMetadataSnapshot());
            if (snapshotDiff.identical.size() > filesToRecoverFromSnapshot) {

                final ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover =
                    new ShardRecoveryPlan.SnapshotFilesToRecover(shardSnapshot.getIndexId(),
                        shardSnapshot.getSnapshotFiles(snapshotDiff.identical));

                plan = buildRecoveryPlan(snapshotFilesToRecover, snapshotDiff.missingAndDifferent);
            }
        }

        return plan;
    }

    private ShardRecoveryPlan buildRecoveryPlan(ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover,
                                                List<StoreFileMetadata> sourceFilesToRecover) {
        return new ShardRecoveryPlan(snapshotFilesToRecover,
            sourceFilesToRecover,
            targetDiff.identical,
            startingSeqNo,
            translogOps,
            recoverySourceMetadata
        );
    }
}
