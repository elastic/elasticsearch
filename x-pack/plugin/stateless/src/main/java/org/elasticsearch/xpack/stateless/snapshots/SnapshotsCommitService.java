/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService.RecoveredCommitsReleasable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.SnapshotShardContextHelper.acquireSnapshotIndexCommit;
import static org.elasticsearch.repositories.SnapshotShardContextHelper.closeSnapshotIndexCommit;
import static org.elasticsearch.repositories.SnapshotShardContextHelper.maybeEnsureNotAborted;
import static org.elasticsearch.repositories.SnapshotShardContextHelper.withSnapshotIndexCommitRef;

/**
 * This class is responsible for tracking and releasing commits needed for ongoing shard snapshots.
 * If a commit is tracked by this class, it is guaranteed to be available in the object store.
 * When a commit is released by this class, the commit becomes a candidate for deletion.
 * Whether it will actually be deleted and when it happens also depend on references from other
 * usages and the shard state, see also {@link StatelessCommitService}.
 */
public class SnapshotsCommitService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SnapshotsCommitService.class);

    static final String SHARD_RELOCATED_TOTAL = "es.repositories.snapshots.shards.relocated.total";
    static final String RECOVERY_RETAINED_COMMITS_HISTOGRAM = "es.repositories.snapshots.shards.recovery_retained_commits.histogram";
    static final String RECOVERY_RETAINED_DURATION_HISTOGRAM = "es.repositories.snapshots.shards.recovery_retained_duration.histogram";

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final StatelessCommitService commitService;
    private final TimeProvider timeProvider;
    private final LongCounter shardRelocationDuringSnapshotCounter;
    private final LongHistogram recoveryRetainedCommitsHistogram;
    private final DoubleHistogram recoveryRetainedDurationHistogram;

    // Per-shard map of snapshot to Releasable that prevents commit deletion. Each Releasable, when closed, releases
    // either a SnapshotIndexCommit or BCC blob references.
    //
    // Shard entries are added via active registration with `registerReleaseForSnapshot` or during recovery
    // (`getExtraReferenceConsumers`, retaining the entire BCC chain). They are removed on (1) shard closed
    // or (2) snapshot completion if no more running snapshot for the shard
    //
    // For each shard entry, its members are released and removed (a) on snapshot completion
    // or (b) on snapshot registration, a new member replaces any old entry of the same snapshot
    // or (c) on shard closed.
    private final Map<ShardId, Map<Snapshot, Releasable>> snapshotsCommitReleasables = new ConcurrentHashMap<>();

    public SnapshotsCommitService(
        ClusterService clusterService,
        IndicesService indicesService,
        StatelessCommitService commitService,
        TimeProvider timeProvider,
        MeterRegistry meterRegistry
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.commitService = commitService;
        this.timeProvider = timeProvider;
        this.shardRelocationDuringSnapshotCounter = meterRegistry.registerLongCounter(
            SHARD_RELOCATED_TOTAL,
            "shard relocated while being snapshotted",
            "unit"
        );
        this.recoveryRetainedCommitsHistogram = meterRegistry.registerLongHistogram(
            RECOVERY_RETAINED_COMMITS_HISTOGRAM,
            "commits retained during recovery for ongoing shard snapshots",
            "unit"
        );
        this.recoveryRetainedDurationHistogram = meterRegistry.registerDoubleHistogram(
            RECOVERY_RETAINED_DURATION_HISTOGRAM,
            "duration till recovery retaining commits are released",
            "s"
        );
    }

    public record SnapshotCommitInfo(
        SnapshotIndexCommit snapshotIndexCommit,
        Map<String, BlobLocation> blobLocations,
        @Nullable String shardStateId
    ) {}

    /**
     * Acquire an index commit for the shard snapshot. If relocation during snapshots is supported, i.e.
     * {@code supportsRelocationDuringSnapshot} = true, also register a releasable for the snapshot so that
     * this class manages the commit's lifecycle. Otherwise, the caller is responsible for releasing the commit.
     */
    public SnapshotCommitInfo acquireAndMaybeRegisterCommitForSnapshot(
        ShardId shardId,
        Snapshot snapshot,
        boolean supportsRelocationDuringSnapshot,
        @Nullable IndexShardSnapshotStatus snapshotStatus // null if the snapshot is running on a remote node
    ) throws IOException {
        ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT);
        final IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        assert indexShard.routingEntry().primary() : "get commit info should only be executed on a primary shard";
        final var snapshotIndexCommitAndShardStateId = acquireSnapshotIndexCommit(
            clusterService,
            indexShard,
            snapshot,
            supportsRelocationDuringSnapshot,
            snapshotStatus
        );
        final var snapshotIndexCommit = snapshotIndexCommitAndShardStateId.snapshotIndexCommit();
        final var shardStateId = snapshotIndexCommitAndShardStateId.shardStateId();
        // Local snapshot (snapshotStatus != null) may be concurrently aborted so that we inc-ref before using the index commit.
        try (var ignored = withSnapshotIndexCommitRef(shardId, snapshot.getSnapshotId(), snapshotIndexCommit, snapshotStatus)) {
            final var indexCommit = snapshotIndexCommit.indexCommit();
            maybeEnsureNotAborted(snapshotStatus);
            final Map<String, BlobLocation> blobLocations = indexCommit.getFileNames()
                .stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        Function.identity(),
                        fileName -> Objects.requireNonNull(commitService.getBlobLocation(shardId, fileName))
                    )
                );
            if (supportsRelocationDuringSnapshot) {
                registerReleaseForSnapshot(shardId, snapshot, snapshotIndexCommit);
            }
            return new SnapshotCommitInfo(snapshotIndexCommit, blobLocations, shardStateId);
        } catch (Exception e) {
            closeSnapshotIndexCommit(snapshotIndexCommit, shardId, snapshot);
            throw e;
        }
    }

    /**
     * Register a releasable for the shard snapshot and the commit it uses. When the releasable closes, the commit is released.
     * This class will always release the specified SnapshotIndexCommit at some point in the future, e.g., shard snapshot completion
     * and shard close. Therefore, the caller is not responsible for the release, i.e., the ownership is transferred to this
     * class.
     */
    // Package private for testing
    void registerReleaseForSnapshot(ShardId shardId, Snapshot snapshot, SnapshotIndexCommit snapshotIndexCommit) {
        final var releasable = Releasables.releaseOnce(() -> closeSnapshotIndexCommit(snapshotIndexCommit, shardId, snapshot));

        final var old = new Releasable[1];
        final var shardReleasables = snapshotsCommitReleasables.compute(shardId, (k, v) -> {
            if (v == null) {
                v = new ConcurrentHashMap<>();
            }
            old[0] = v.put(snapshot, releasable);
            return v;
        });
        Releasables.close(old); // close any old releasable that maybe created on recovery or previous register attempt

        // The shard may concurrently close right before the above releasable is added, check the shard closed status again to repair
        if (commitService.isShardClosed(shardId)) {
            logger.debug("shard {} concurrently closed while registering snapshot [{}] commit releasable", shardId, snapshot);
            releaseCommitsAndRemoveShardAfterShardClosed(shardId);
        } else if (isShardSnapshotReadingData(SnapshotsInProgress.get(clusterService.state()).snapshot(snapshot), shardId) == false) {
            // If the snapshotting node is a remote node and lagging, master can remove it and concurrently mark the shard snapshot
            // as FAILED during the registration attempt.
            logger.debug("shard {} snapshot [{}] concurrently completed while registering commit releasable", shardId, snapshot);
            Releasables.close(shardReleasables.remove(snapshot));
            if (shardReleasables.isEmpty()) {
                removeShardIfNoTrackedSnapshot(shardId);
            }
            throw new IndexShardSnapshotFailedException(shardId, "snapshot [" + snapshot + "] is no longer active");
        }
    }

    /**
     * Callback on shard closed. Remove the shard from tracking and release all its retained commits for snapshots.
     */
    public void releaseCommitsAndRemoveShardAfterShardClosed(ShardId shardId) {
        assert commitService.isShardClosed(shardId) : "shard [" + shardId + "] is not closed";
        final var shardReleasables = snapshotsCommitReleasables.remove(shardId);
        if (shardReleasables != null) {
            logger.debug("release snapshot commits for shard {} after shard close", shardId);
            shardRelocationDuringSnapshotCounter.increment();
            Releasables.close(shardReleasables.values());
        }
    }

    /**
     * For each running shard snapshot for the given shard, creates a consumer that expects to receive a releasable.
     * When the releasable is closed, it dec-refs on all commits that were inc-refed for creating it.
     * It is used as a callback at recovery time to add additional refcounts to pre-recovery commits.
     */
    public Iterator<Consumer<RecoveredCommitsReleasable>> getExtraReferenceConsumers(ShardId shardId) {
        return Iterators.map(getRunningSnapshots(shardId), snapshot -> originalReleasable -> {
            recoveryRetainedCommitsHistogram.record(originalReleasable.size());
            final long startTime = timeProvider.relativeTimeInMillis();
            final var releasable = Releasables.releaseOnce(
                Releasables.wrap(
                    originalReleasable,
                    () -> recoveryRetainedDurationHistogram.record((timeProvider.relativeTimeInMillis() - startTime) / 1_000d)
                )
            );

            logger.debug("{} conservatively retaining commits for running snapshot [{}]", shardId, snapshot);
            final var shardReleasables = snapshotsCommitReleasables.compute(shardId, (k, v) -> {
                if (v == null) {
                    v = new ConcurrentHashMap<>();
                }
                final var old = v.put(snapshot, releasable);
                assert old == null : "shard " + shardId + " has unexpected tracking for snapshot " + snapshot + " before recovery";
                return v;
            });

            // The snapshot may concurrently complete while the shard is recovering right before the above releasable is added.
            // So we check snapshot completion again to repair for it.
            final var snapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
            if (isShardSnapshotReadingData(snapshotsInProgress.snapshot(snapshot), shardId) == false) {
                logger.debug("shard {} snapshot [{}] concurrently completed while retaining commits during recovery", shardId, snapshot);
                Releasables.close(shardReleasables.remove(snapshot));
                if (shardReleasables.isEmpty()) {
                    removeShardIfNoTrackedSnapshot(shardId);
                }
            }
        });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // Iterate through all tracked snapshots and release those that no longer actively reading shard data
        final var snapshotsInProgress = SnapshotsInProgress.get(event.state());
        if (snapshotsInProgress.equals(SnapshotsInProgress.get(event.previousState())) == false) {
            // ConcurrentHashMap's iterator is not strongly consistent. That's OK here since we don't need it here either.
            snapshotsCommitReleasables.forEach((shardId, shardReleasables) -> {
                shardReleasables.forEach((snapshot, releasable) -> {
                    if (isShardSnapshotReadingData(snapshotsInProgress.snapshot(snapshot), shardId) == false) {
                        // Be paranoid and remove snapshot if it still has the same releasable
                        if (shardReleasables.remove(snapshot, releasable)) {
                            Releasables.close(releasable);
                        }
                    }
                });
                if (shardReleasables.isEmpty()) {
                    removeShardIfNoTrackedSnapshot(shardId);
                }
            });
        }
    }

    private void removeShardIfNoTrackedSnapshot(ShardId shardId) {
        snapshotsCommitReleasables.compute(shardId, (k, v) -> v == null ? null : (v.isEmpty() ? null : v));
    }

    private Iterator<Snapshot> getRunningSnapshots(ShardId shardId) {
        return Iterators.flatMap(
            SnapshotsInProgress.get(clusterService.state()).entriesByRepo().iterator(),
            entries -> Iterators.map(
                Iterators.filter(entries.iterator(), entry -> isShardSnapshotReadingData(entry, shardId)),
                SnapshotsInProgress.Entry::snapshot
            )
        );
    }

    private static boolean isShardSnapshotReadingData(SnapshotsInProgress.Entry snapshotEntry, ShardId shardId) {
        if (snapshotEntry == null || snapshotEntry.isClone() || snapshotEntry.state().completed()) {
            return false;
        }
        final var shardSnapshotStatus = snapshotEntry.shards().get(shardId);
        // Both INIT and ABORTED shard snapshot may need to read existing shard data
        return shardSnapshotStatus != null
            && (shardSnapshotStatus.state() == SnapshotsInProgress.ShardState.INIT
                || shardSnapshotStatus.state() == SnapshotsInProgress.ShardState.ABORTED);
    }

    // package private for testing only
    boolean hasTrackingForShard(ShardId shardId) {
        return snapshotsCommitReleasables.containsKey(shardId);
    }

    // package private for testing only
    void assertEmptyTracking() {
        assert snapshotsCommitReleasables.isEmpty() : "tracking is not empty: " + snapshotsCommitReleasables;
    }
}
