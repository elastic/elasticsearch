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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

public class InternalSnapshotsInfoService implements ClusterStateListener, SnapshotsInfoService {

    public static final Setting<Integer> INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING = Setting.intSetting(
        "cluster.snapshot.info.max_concurrent_fetches",
        5,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(InternalSnapshotsInfoService.class);

    private static final ActionListener<Void> REROUTE_LISTENER = ActionListener.wrap(
        r -> logger.trace("reroute after snapshot shard size update completed"),
        e -> logger.debug("reroute after snapshot shard size update failed", e)
    );

    private final ThreadPool threadPool;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Supplier<RerouteService> rerouteService;

    /** contains the snapshot shards for which the size is known **/
    // volatile for the unlocked access in numberOfKnownSnapshotShardSizes()
    // map itself is immutable
    private volatile ImmutableOpenMap<SnapshotShard, Long> knownSnapshotShards;

    // all access is guarded by mutex
    private boolean isMaster;

    /** contains the snapshot shards for which the size is unknown and must be fetched (or is being fetched) **/
    private final Set<SnapshotShard> unknownSnapshotShards;

    /** a blocking queue used for concurrent fetching **/
    private final Queue<SnapshotShard> queue;

    /** contains the snapshot shards for which the snapshot shard size retrieval failed **/
    private final Set<SnapshotShard> failedSnapshotShards;

    private volatile int maxConcurrentFetches;
    private int activeFetches;

    private final Object mutex;

    public InternalSnapshotsInfoService(
        final Settings settings,
        final ClusterService clusterService,
        final Supplier<RepositoriesService> repositoriesServiceSupplier,
        final Supplier<RerouteService> rerouteServiceSupplier
    ) {
        this.threadPool = clusterService.getClusterApplierService().threadPool();
        this.repositoriesService = repositoriesServiceSupplier;
        this.rerouteService = rerouteServiceSupplier;
        this.knownSnapshotShards = ImmutableOpenMap.of();
        this.unknownSnapshotShards = new LinkedHashSet<>();
        this.failedSnapshotShards = new LinkedHashSet<>();
        this.queue = new ArrayDeque<>();
        this.mutex = new Object();
        this.activeFetches = 0;
        this.maxConcurrentFetches = INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING.get(settings);
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING, this::setMaxConcurrentFetches);
        if (DiscoveryNode.isMasterNode(settings)) {
            clusterService.addListener(this);
        }
    }

    private void setMaxConcurrentFetches(Integer maxConcurrentFetches) {
        this.maxConcurrentFetches = maxConcurrentFetches;
    }

    @Override
    public SnapshotShardSizeInfo snapshotShardSizes() {
        synchronized (mutex) {
            final ImmutableOpenMap.Builder<SnapshotShard, Long> snapshotShardSizes = ImmutableOpenMap.builder(knownSnapshotShards);
            if (failedSnapshotShards.isEmpty() == false) {
                for (SnapshotShard snapshotShard : failedSnapshotShards) {
                    Long previous = snapshotShardSizes.put(snapshotShard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                    assert previous == null : "snapshot shard size already known for " + snapshotShard;
                }
            }
            return new SnapshotShardSizeInfo(snapshotShardSizes.build());
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            if (event.previousState().nodes().isLocalNodeElectedMaster()
                && event.routingTableChanged() == false
                && event.nodesChanged() == false) {
                // we neither just became master nor did the routing table change, nothing to update
                return;
            }
            final Set<SnapshotShard> onGoingSnapshotRecoveries = listOfSnapshotShards(event.state());

            int unknownShards = 0;
            synchronized (mutex) {
                isMaster = true;
                for (SnapshotShard snapshotShard : onGoingSnapshotRecoveries) {
                    // check if already populated entry
                    if (knownSnapshotShards.containsKey(snapshotShard) == false && failedSnapshotShards.contains(snapshotShard) == false) {
                        // check if already fetching snapshot info in progress
                        if (unknownSnapshotShards.add(snapshotShard)) {
                            queue.add(snapshotShard);
                            unknownShards += 1;
                        }
                    }
                }
                // Clean up keys from knownSnapshotShardSizes that are no longer needed for recoveries
                cleanUpSnapshotShardSizes(onGoingSnapshotRecoveries);
            }

            final int nbFetchers = Math.min(unknownShards, maxConcurrentFetches);
            for (int i = 0; i < nbFetchers; i++) {
                fetchNextSnapshotShard();
            }

        } else if (event.previousState().nodes().isLocalNodeElectedMaster()) {
            // TODO Maybe just clear out non-ongoing snapshot recoveries is the node is master eligible, so that we don't
            // have to repopulate the data over and over in an unstable master situation?
            synchronized (mutex) {
                // information only needed on current master
                knownSnapshotShards = ImmutableOpenMap.of();
                failedSnapshotShards.clear();
                isMaster = false;
                SnapshotShard snapshotShard;
                while ((snapshotShard = queue.poll()) != null) {
                    final boolean removed = unknownSnapshotShards.remove(snapshotShard);
                    assert removed : "snapshot shard to remove does not exist " + snapshotShard;
                }
                assert invariant();
            }
        } else {
            synchronized (mutex) {
                assert unknownSnapshotShards.isEmpty() || unknownSnapshotShards.size() == activeFetches;
                assert knownSnapshotShards.isEmpty();
                assert failedSnapshotShards.isEmpty();
                assert isMaster == false;
                assert queue.isEmpty();
            }
        }
    }

    private void fetchNextSnapshotShard() {
        synchronized (mutex) {
            if (activeFetches < maxConcurrentFetches) {
                final SnapshotShard snapshotShard = queue.poll();
                if (snapshotShard != null) {
                    activeFetches += 1;
                    threadPool.generic().execute(new FetchingSnapshotShardSizeRunnable(snapshotShard));
                }
            }
            assert invariant();
        }
    }

    private class FetchingSnapshotShardSizeRunnable extends AbstractRunnable {

        private final SnapshotShard snapshotShard;
        private boolean removed;

        FetchingSnapshotShardSizeRunnable(SnapshotShard snapshotShard) {
            super();
            this.snapshotShard = snapshotShard;
            this.removed = false;
        }

        @Override
        protected void doRun() throws Exception {
            final RepositoriesService repositories = repositoriesService.get();
            assert repositories != null;
            final Repository repository = repositories.repository(snapshotShard.snapshot.getRepository());

            logger.debug("fetching snapshot shard size for {}", snapshotShard);
            final long snapshotShardSize = repository.getShardSnapshotStatus(
                snapshotShard.snapshot().getSnapshotId(),
                snapshotShard.index(),
                snapshotShard.shardId()
            ).asCopy().getTotalSize();

            logger.debug("snapshot shard size for {}: {} bytes", snapshotShard, snapshotShardSize);

            boolean updated = false;
            synchronized (mutex) {
                removed = unknownSnapshotShards.remove(snapshotShard);
                assert removed : "snapshot shard to remove does not exist " + snapshotShardSize;
                if (isMaster) {
                    final ImmutableOpenMap.Builder<SnapshotShard, Long> newSnapshotShardSizes = ImmutableOpenMap.builder(
                        knownSnapshotShards
                    );
                    updated = newSnapshotShardSizes.put(snapshotShard, snapshotShardSize) == null;
                    assert updated : "snapshot shard size already exists for " + snapshotShard;
                    knownSnapshotShards = newSnapshotShardSizes.build();
                }
                activeFetches -= 1;
                assert invariant();
            }
            if (updated) {
                rerouteService.get().reroute("snapshot shard size updated", Priority.HIGH, REROUTE_LISTENER);
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(() -> format("failed to retrieve shard size for %s", snapshotShard), e);
            boolean failed = false;
            synchronized (mutex) {
                if (isMaster) {
                    failed = failedSnapshotShards.add(snapshotShard);
                    assert failed : "snapshot shard size already failed for " + snapshotShard;
                }
                if (removed == false) {
                    unknownSnapshotShards.remove(snapshotShard);
                }
                activeFetches -= 1;
                assert invariant();
            }
            if (failed) {
                rerouteService.get().reroute("snapshot shard size failed", Priority.HIGH, REROUTE_LISTENER);
            }
        }

        @Override
        public void onAfter() {
            fetchNextSnapshotShard();
        }
    }

    private void cleanUpSnapshotShardSizes(Set<SnapshotShard> requiredSnapshotShards) {
        assert Thread.holdsLock(mutex);
        ImmutableOpenMap.Builder<SnapshotShard, Long> newSnapshotShardSizes = null;
        for (SnapshotShard shard : knownSnapshotShards.keySet()) {
            if (requiredSnapshotShards.contains(shard) == false) {
                if (newSnapshotShardSizes == null) {
                    newSnapshotShardSizes = ImmutableOpenMap.builder(knownSnapshotShards);
                }
                newSnapshotShardSizes.remove(shard);
            }
        }
        if (newSnapshotShardSizes != null) {
            knownSnapshotShards = newSnapshotShardSizes.build();
        }
        failedSnapshotShards.retainAll(requiredSnapshotShards);
    }

    private boolean invariant() {
        assert Thread.holdsLock(mutex);
        assert activeFetches >= 0 : "active fetches should be greater than or equal to zero but got: " + activeFetches;
        assert activeFetches <= maxConcurrentFetches : activeFetches + " <= " + maxConcurrentFetches;
        for (SnapshotShard shard : knownSnapshotShards.keySet()) {
            assert unknownSnapshotShards.contains(shard) == false : "cannot be known and unknown at same time: " + shard;
            assert failedSnapshotShards.contains(shard) == false : "cannot be known and failed at same time: " + shard;
        }
        for (SnapshotShard shard : unknownSnapshotShards) {
            assert knownSnapshotShards.keySet().contains(shard) == false : "cannot be unknown and known at same time: " + shard;
            assert failedSnapshotShards.contains(shard) == false : "cannot be unknown and failed at same time: " + shard;
        }
        for (SnapshotShard shard : failedSnapshotShards) {
            assert knownSnapshotShards.keySet().contains(shard) == false : "cannot be failed and known at same time: " + shard;
            assert unknownSnapshotShards.contains(shard) == false : "cannot be failed and unknown at same time: " + shard;
        }
        return true;
    }

    // used in tests
    int numberOfUnknownSnapshotShardSizes() {
        synchronized (mutex) {
            return unknownSnapshotShards.size();
        }
    }

    // used in tests
    int numberOfFailedSnapshotShardSizes() {
        synchronized (mutex) {
            return failedSnapshotShards.size();
        }
    }

    // used in tests
    int numberOfKnownSnapshotShardSizes() {
        return knownSnapshotShards.size();
    }

    private static Set<SnapshotShard> listOfSnapshotShards(final ClusterState state) {
        final Set<SnapshotShard> snapshotShards = new HashSet<>();
        for (ShardRouting shardRouting : state.getRoutingNodes().unassigned()) {
            if (shardRouting.primary() && shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                final RecoverySource.SnapshotRecoverySource snapshotRecoverySource = (RecoverySource.SnapshotRecoverySource) shardRouting
                    .recoverySource();
                final SnapshotShard snapshotShard = new SnapshotShard(
                    snapshotRecoverySource.snapshot(),
                    snapshotRecoverySource.index(),
                    shardRouting.shardId()
                );
                snapshotShards.add(snapshotShard);
            }
        }
        return Collections.unmodifiableSet(snapshotShards);
    }

    public record SnapshotShard(Snapshot snapshot, IndexId index, ShardId shardId) {
        @Override
        public String toString() {
            return "[" + "snapshot=" + snapshot + ", index=" + index + ", shard=" + shardId + ']';
        }
    }
}
