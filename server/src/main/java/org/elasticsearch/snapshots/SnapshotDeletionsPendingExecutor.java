/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsPending;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;
import static org.elasticsearch.snapshots.SnapshotUtils.cloneSources;
import static org.elasticsearch.snapshots.SnapshotUtils.deletionsSources;
import static org.elasticsearch.snapshots.SnapshotUtils.restoreSources;
import static org.elasticsearch.snapshots.SnapshotsService.findRepositoryForPendingDeletion;

public class SnapshotDeletionsPendingExecutor {

    private static final Logger logger = LogManager.getLogger(SnapshotDeletionsPendingExecutor.class);

    public static final Setting<TimeValue> PENDING_SNAPSHOT_DELETIONS_RETRY_INTERVAL_SETTING = Setting.timeSetting(
        "snapshot.snapshot_deletions_pending.retry_interval",
        TimeValue.timeValueSeconds(30L),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> PENDING_SNAPSHOT_DELETIONS_EXPIRATION_INTERVAL_SETTING = Setting.timeSetting(
        "snapshot.snapshot_deletions_pending.expiration_interval",
        TimeValue.timeValueHours(12L),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Set of pending snapshots deletions whose deletion is already triggered
     */
    private final Set<SnapshotId> triggered = ConcurrentCollections.newConcurrentSet();

    /**
     * Map of pending snapshots deletions whose deletion is conflicting with on-going restores/clones/repository clean up or repository
     * missing or read-only. Those sets are used to identify the cluster state updates to process in case they resolve some conflict.
     */
    private final Map<SnapshotId, ConflictType> conflicting = new HashMap<>();

    /**
     * Counters for the type of conflicts for the current set of conflicting pending snapshots deletions. This is used to look for
     * updates in cluster state updates only when it is really needed.
     */
    enum ConflictType {
        RESTORING,
        CLONING,
        REPO_MISSING,
        REPO_READONLY,
        REPO_CLEANUP
    }

    private final SnapshotsService snapshotsService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    private volatile TimeValue pendingDeletionsRetryInterval;
    private volatile TimeValue pendingDeletionsExpirationInterval;

    SnapshotDeletionsPendingExecutor(
        SnapshotsService snapshotsService,
        ClusterService clusterService,
        ThreadPool threadPool,
        Settings settings
    ) {
        this.snapshotsService = Objects.requireNonNull(snapshotsService);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.threadPool = Objects.requireNonNull(threadPool);
        pendingDeletionsRetryInterval = PENDING_SNAPSHOT_DELETIONS_RETRY_INTERVAL_SETTING.get(settings);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(PENDING_SNAPSHOT_DELETIONS_RETRY_INTERVAL_SETTING, t -> pendingDeletionsRetryInterval = t);
        pendingDeletionsExpirationInterval = PENDING_SNAPSHOT_DELETIONS_EXPIRATION_INTERVAL_SETTING.get(settings);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(PENDING_SNAPSHOT_DELETIONS_EXPIRATION_INTERVAL_SETTING, t -> pendingDeletionsExpirationInterval = t);
    }

    /**
     * Find snapshots to delete in the the cluster state and triggers explicit snapshot delete requests. This method attempts to detect
     * conflicting situations where triggering the snapshot deletion would likely fail due to a concurrent snapshot operation. In such
     * cases the snapshot deletion is not triggered as it should be triggered by subsequent cluster state updates once the conflicting
     * situation is resolved.
     *
     * The repository name and uuid information are extracted from the {@link SnapshotDeletionsPending} entries in order to find the
     * repository to execute the snapshot delete request against. If the repo uuid was known at the time the snapshot was added to
     * {@link SnapshotDeletionsPending} we try to find the corresponding repository, or a repository with a missing uuid but the same
     * name. If the repo uuid was not known at the time the snapshot was added to {@link SnapshotDeletionsPending}, we try to find a
     * repository with the same name.
     *
     * @param state the current {@link ClusterState}
     * @param previousState the previous {@link ClusterState}
     */
    public synchronized void processPendingDeletions(ClusterState state, ClusterState previousState) {
        assert Thread.currentThread().getName().contains('[' + ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME + ']')
            || Thread.currentThread().getName().startsWith("TEST-") : Thread.currentThread().getName();

        final SnapshotDeletionsPending snapshotDeletionsPending = state.custom(SnapshotDeletionsPending.TYPE);
        if (state.nodes().isLocalNodeElectedMaster() == false || snapshotDeletionsPending == null || snapshotDeletionsPending.isEmpty()) {
            clearConflicts();
            return;
        }

        if (pendingDeletionsChanged(state, previousState) || pendingDeletionsWithConflictsChanged(state, previousState)) {
            final RepositoriesMetadata repositories = state.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
            final RepositoryCleanupInProgress cleanUps = state.custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY);

            final Set<SnapshotId> currentDeletions = deletionsSources(state);
            final Set<SnapshotId> currentRestores = restoreSources(state);
            final Set<SnapshotId> currentClones = cloneSources(state);

            // the snapshots to trigger deletion for, per repository
            final Map<RepositoryMetadata, Map<SnapshotId, Long>> snapshotsToDelete = new HashMap<>();

            for (SnapshotDeletionsPending.Entry snapshot : snapshotDeletionsPending.entries()) {
                final SnapshotId snapshotId = snapshot.getSnapshotId();

                if (currentRestores.contains(snapshotId)) {
                    logger.trace("snapshot to delete [{}] is being restored, waiting for restore to complete", snapshotId);
                    conflicting.put(snapshotId, ConflictType.RESTORING);
                    continue;
                }
                if (currentClones.contains(snapshotId)) {
                    logger.trace("snapshot to delete [{}] is being cloned, waiting for cloning to complete", snapshotId);
                    conflicting.put(snapshotId, ConflictType.CLONING);
                    continue;
                }
                if (cleanUps.hasCleanupInProgress()) {
                    if (conflicting.put(snapshotId, ConflictType.REPO_CLEANUP) != ConflictType.REPO_CLEANUP) {
                        logger.debug(
                            "a repository clean-up is in progress, cannot delete pending snapshot [{}] created at {}",
                            snapshotId,
                            Instant.ofEpochMilli(snapshot.getIndexDeletionTime()).atZone(ZoneOffset.UTC)
                        );
                    }
                    continue;
                }
                if (currentDeletions.contains(snapshotId)) {
                    logger.trace("snapshot to delete [{}] is already queued", snapshotId);
                    conflicting.remove(snapshotId);
                    continue;
                }

                final Optional<RepositoryMetadata> optionalRepository = findRepositoryForPendingDeletion(
                    repositories,
                    snapshot.getRepositoryName(),
                    snapshot.getRepositoryUuid()
                );
                if (optionalRepository.isEmpty()) {
                    if (conflicting.put(snapshotId, ConflictType.REPO_MISSING) != ConflictType.REPO_MISSING) {
                        logger.debug(
                            "repository [{}/{}] not found, cannot delete pending snapshot [{}] created at {}",
                            snapshot.getRepositoryName(),
                            snapshot.getRepositoryUuid(),
                            snapshotId,
                            Instant.ofEpochMilli(snapshot.getIndexDeletionTime()).atZone(ZoneOffset.UTC)
                        );
                    }
                    continue;
                }
                final RepositoryMetadata repository = optionalRepository.get();
                if (repository.settings().getAsBoolean(READONLY_SETTING_KEY, false)) {
                    if (conflicting.put(snapshotId, ConflictType.REPO_READONLY) != ConflictType.REPO_READONLY) {
                        logger.debug(
                            "repository [{}/{}] is read-only, cannot delete pending snapshot [{}] created at {}",
                            repository.name(),
                            repository.uuid(),
                            snapshotId,
                            Instant.ofEpochMilli(snapshot.getIndexDeletionTime()).atZone(ZoneOffset.UTC)
                        );
                    }
                    continue;
                }
                conflicting.remove(snapshotId);

                if (triggered.add(snapshotId)) {
                    logger.info("triggering snapshot deletion for [{}]", snapshotId);
                    final Long previous = snapshotsToDelete.computeIfAbsent(repository, r -> new HashMap<>())
                        .put(snapshotId, snapshot.getIndexDeletionTime());
                    assert previous == null : snapshotId;
                }
            }

            assert snapshotDeletionsPending.entries()
                .stream()
                .map(SnapshotDeletionsPending.Entry::getSnapshotId)
                .allMatch(snapId -> triggered.contains(snapId) || conflicting.containsKey(snapId) || currentDeletions.contains(snapId));

            snapshotsToDelete.forEach(
                (repo, snapshots) -> threadPool.generic().execute(new SnapshotsToDeleteRunnable(repo.name(), repo.uuid(), snapshots))
            );
        }
        assert Sets.intersection(triggered, conflicting.keySet()).isEmpty()
            : "pending snapshot deletion cannot be both triggered and in conflict: " + triggered + " vs " + conflicting.keySet();
        assert conflicting.keySet().stream().allMatch(snapshotDeletionsPending::contains);
    }

    // only used in tests
    boolean isTriggered(SnapshotId snapshotId) {
        return triggered.contains(snapshotId);
    }

    // only used in tests
    synchronized ConflictType getConflict(SnapshotId snapshotId) {
        return conflicting.get(snapshotId);
    }

    synchronized void clearConflicts() {
        if (conflicting.isEmpty() == false) {
            conflicting.clear();
        }
    }

    // package-private for testing
    synchronized boolean pendingDeletionsChanged(ClusterState state, ClusterState previousState) {
        SnapshotDeletionsPending previous = previousState.custom(SnapshotDeletionsPending.TYPE, SnapshotDeletionsPending.EMPTY);
        SnapshotDeletionsPending current = state.custom(SnapshotDeletionsPending.TYPE, SnapshotDeletionsPending.EMPTY);
        return Objects.equals(previous, current) == false;
    }

    // package-private for testing
    synchronized boolean pendingDeletionsWithConflictsChanged(ClusterState state, ClusterState previousState) {
        if (conflicting.values().stream().anyMatch(c -> c == ConflictType.RESTORING)) {
            RestoreInProgress previous = previousState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
            RestoreInProgress current = state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
            if (Objects.equals(previous, current) == false) {
                return true;
            }
        }
        if (conflicting.values().stream().anyMatch(c -> c == ConflictType.CLONING)) {
            Set<SnapshotsInProgress.Entry> previous = previousState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .asStream()
                .filter(SnapshotsInProgress.Entry::isClone)
                .collect(Collectors.toSet());
            Set<SnapshotsInProgress.Entry> current = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .asStream()
                .filter(SnapshotsInProgress.Entry::isClone)
                .collect(Collectors.toSet());
            if (Objects.equals(previous, current) == false) {
                return true;
            }
        }
        if (conflicting.values().stream().anyMatch(c -> c == ConflictType.REPO_MISSING || c == ConflictType.REPO_READONLY)) {
            RepositoriesMetadata previous = previousState.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
            RepositoriesMetadata current = state.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
            if (previous.equals(current) == false) {
                return true;
            }
        }
        if (conflicting.values().stream().anyMatch(c -> c == ConflictType.REPO_CLEANUP)) {
            boolean previousCleanUp = previousState.custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY)
                .hasCleanupInProgress();
            boolean currentCleanUp = state.custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY)
                .hasCleanupInProgress();
            return previousCleanUp != currentCleanUp;
        }
        return false;
    }

    private boolean isExpiredPendingDeletion(long deletionTimeEpochMillis) {
        return Instant.ofEpochMilli(deletionTimeEpochMillis)
            .plusMillis(pendingDeletionsExpirationInterval.getMillis())
            .isBefore(Instant.now());
    }

    /**
     * A {@link Runnable} used to process the deletion of snapshots marked as to delete for a given repository.
     */
    private class SnapshotsToDeleteRunnable extends AbstractRunnable {

        private final Map<SnapshotId, Long> snapshots;
        private final String repositoryName;
        private final String repositoryUuid;
        private final boolean missingUuid;

        SnapshotsToDeleteRunnable(String repositoryName, String repositoryUuid, Map<SnapshotId, Long> snapshots) {
            this.repositoryName = Objects.requireNonNull(repositoryName);
            this.repositoryUuid = Objects.requireNonNull(repositoryUuid);
            this.snapshots = Objects.requireNonNull(snapshots);
            this.missingUuid = RepositoryData.MISSING_UUID.equals(repositoryUuid);
        }

        @Override
        protected void doRun() throws Exception {
            final Set<SnapshotId> pendingDeletionsToRemove = ConcurrentCollections.newConcurrentSet();
            final CountDown countDown = new CountDown(snapshots.size());

            for (Map.Entry<SnapshotId, Long> snapshot : snapshots.entrySet()) {
                final SnapshotId snapshotId = snapshot.getKey();
                final ActionListener<Void> listener = new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void unused) {
                        logger.debug(
                            "snapshot marked as to delete [{}] successfully deleted from repository [{}/{}]",
                            snapshotId,
                            repositoryName,
                            repositoryUuid
                        );
                        pendingDeletionsToRemove.add(snapshotId);
                        finish();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e instanceof SnapshotMissingException && missingUuid == false) {
                            pendingDeletionsToRemove.add(snapshotId);
                            logger.debug(
                                () -> new ParameterizedMessage(
                                    "snapshot marked as to delete [{}] is missing in repository [{}/{}], removing from pending deletions",
                                    snapshotId,
                                    repositoryName,
                                    repositoryUuid
                                ),
                                e
                            );
                        } else if (isExpiredPendingDeletion(snapshot.getValue())) {
                            pendingDeletionsToRemove.add(snapshotId);
                            logger.warn(
                                () -> new ParameterizedMessage(
                                    "snapshot marked as to delete [{}] failed to be deleted within [{}]. The pending snapshot "
                                        + "expired before the snapshot could be deleted from the repository and as such might still "
                                        + "exist in the original repository [{}/{}]. This snapshot will now be removed from the list of "
                                        + "pending deletions.",
                                    snapshotId,
                                    pendingDeletionsExpirationInterval,
                                    repositoryName,
                                    repositoryUuid
                                ),
                                e
                            );
                        } else {
                            logger.debug(
                                () -> new ParameterizedMessage(
                                    "[{}/{}] attempt to delete snapshot marked as to delete [{}] failed; deletion will be retried in [{}]",
                                    repositoryName,
                                    repositoryUuid,
                                    snapshotId,
                                    pendingDeletionsRetryInterval
                                ),
                                e
                            );
                        }
                        finish();
                    }

                    void finish() {
                        if (countDown.countDown()) {
                            final Map<SnapshotId, Long> retryables = snapshots.entrySet()
                                .stream()
                                .filter(snap -> pendingDeletionsToRemove.contains(snap.getKey()) == false)
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                            if (retryables.isEmpty() == false) {
                                // TODO maybe re-resolve repository here if the uuid is missing?
                                threadPool.scheduleUnlessShuttingDown(
                                    pendingDeletionsRetryInterval,
                                    ThreadPool.Names.GENERIC,
                                    new SnapshotsToDeleteRunnable(repositoryName, repositoryUuid, retryables)
                                );
                            }
                            if (pendingDeletionsToRemove.isEmpty() == false) {
                                clusterService.submitStateUpdateTask("remove-snapshot-deletions-in-pending", new ClusterStateUpdateTask() {
                                    @Override
                                    public ClusterState execute(ClusterState currentState) {
                                        final SnapshotDeletionsPending currentPendings = currentState.custom(
                                            SnapshotDeletionsPending.TYPE,
                                            SnapshotDeletionsPending.EMPTY
                                        );
                                        final SnapshotDeletionsPending updatedPendings = currentPendings.withRemovedSnapshots(
                                            List.copyOf(pendingDeletionsToRemove)
                                        );
                                        if (currentPendings == updatedPendings) {
                                            return currentState;
                                        }
                                        return ClusterState.builder(currentState)
                                            .putCustom(SnapshotDeletionsPending.TYPE, updatedPendings)
                                            .build();
                                    }

                                    @Override
                                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                                        triggered.removeAll(pendingDeletionsToRemove);
                                    }

                                    @Override
                                    public void onFailure(String source, Exception e) {
                                        triggered.removeAll(pendingDeletionsToRemove);
                                    }
                                }, ClusterStateTaskExecutor.unbatched());
                            }
                        }
                    }
                };

                try {
                    snapshotsService.deleteSnapshotsByUuid(repositoryName, new String[] { snapshotId.getUUID() }, listener);
                } catch (Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage("[{}] failed to trigger deletion of snapshot [{}]", repositoryName, snapshotId),
                        e
                    );
                    listener.onFailure(e);
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            triggered.removeAll(snapshots.keySet());
            logger.warn(
                () -> new ParameterizedMessage("[{}] failed to trigger deletion of snapshots {}", repositoryName, snapshots.keySet()),
                e
            );
        }
    }
}
