/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class InternalSnapshotsInfoService implements ClusterStateListener, SnapshotsInfoService {

    public static final Setting<Integer> INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING =
        Setting.intSetting("cluster.snapshot.info.max_concurrent_fetches", 5, 1,
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    private static final Logger logger = LogManager.getLogger(InternalSnapshotsInfoService.class);

    private static final ActionListener<ClusterState> REROUTE_LISTENER = ActionListener.wrap(
        r -> logger.trace("reroute after snapshot shard size update completed"),
        e -> logger.debug("reroute after snapshot shard size update failed", e)
    );

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Supplier<RerouteService> rerouteService;

    /** contains the snapshot shards for which the size is known **/
    private volatile ImmutableOpenMap<SnapshotShard, Long> knownSnapshotShardSizes;
    private volatile boolean isMaster;

    /** contains the snapshot shards for which the size is unknown and must be fetched (or is being fetched) **/
    private final Set<SnapshotShard> unknownSnapshotShards;

    /** a blocking queue used for concurrent fetching **/
    private final BlockingQueue<SnapshotShard> queue;

    private volatile int maxConcurrentFetches;
    private final AtomicInteger concurrentFetches;

    public InternalSnapshotsInfoService(
        final Settings settings,
        final ClusterService clusterService,
        final Supplier<RepositoriesService> repositoriesServiceSupplier,
        final Supplier<RerouteService> rerouteServiceSupplier
    ) {
        this.clusterService = clusterService;
        this.threadPool = clusterService.getClusterApplierService().threadPool();
        this.repositoriesService = repositoriesServiceSupplier;
        this.rerouteService = rerouteServiceSupplier;
        this.knownSnapshotShardSizes = ImmutableOpenMap.of();
        this.unknownSnapshotShards  = Sets.newConcurrentHashSet();
        this.queue = new LinkedBlockingQueue<>();
        this.concurrentFetches = new AtomicInteger(0);
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
        return new SnapshotShardSizeInfo(knownSnapshotShardSizes);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            final Set<SnapshotShard> onGoingSnapshotRecoveries = new HashSet<>();

            int unknownShards = 0;
            for (ShardRouting shardRouting : event.state().routingTable().shardsWithState(ShardRoutingState.UNASSIGNED)) {
                if (shardRouting.primary() && shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                    final RecoverySource.SnapshotRecoverySource snapshotRecoverySource =
                        (RecoverySource.SnapshotRecoverySource) shardRouting.recoverySource();
                    final SnapshotShard snapshotShard = new SnapshotShard(snapshotRecoverySource.snapshot(),
                        snapshotRecoverySource.index(), shardRouting.shardId());
                    onGoingSnapshotRecoveries.add(snapshotShard);

                    // check if already populated entry
                    if (knownSnapshotShardSizes.containsKey(snapshotShard) == false) {
                        // check if already fetching snapshot info in progress
                        if (unknownSnapshotShards.add(snapshotShard)) {
                            queue.add(snapshotShard);
                            unknownShards += 1;
                        }
                    }
                }
            }

            // Clean up keys from knownSnapshotShardSizes that are no longer needed for recoveries
            synchronized (this) {
                isMaster = true;
                ImmutableOpenMap.Builder<SnapshotShard, Long> newSnapshotShardSizes = null;
                for (ObjectCursor<SnapshotShard> shard : knownSnapshotShardSizes.keys()) {
                    if (onGoingSnapshotRecoveries.contains(shard.value) == false) {
                        if (newSnapshotShardSizes == null) {
                            newSnapshotShardSizes = ImmutableOpenMap.builder(knownSnapshotShardSizes);
                        }
                        newSnapshotShardSizes.remove(shard.value);
                    }
                }
                if (newSnapshotShardSizes != null) {
                    knownSnapshotShardSizes = newSnapshotShardSizes.build();
                }
            }

            final int nbFetchers = Math.min(unknownShards, maxConcurrentFetches);
            for (int i = 0; i < nbFetchers; i++) {
                final int activeFetches = concurrentFetches.incrementAndGet();
                if (activeFetches < maxConcurrentFetches + 1) {
                    fetchNextSnapshotShard();
                } else {
                    final int value = concurrentFetches.decrementAndGet();
                    assert value >= 0 : "Unexpected value: " + value;
                }
            }
        } else {
            synchronized (this) {
                // information only needed on current master
                knownSnapshotShardSizes = ImmutableOpenMap.of();
                isMaster = false;
            }
        }
    }

    private void fetchNextSnapshotShard() {
        boolean success = false;
        try {
            final SnapshotShard snapshotShard = queue.poll(0L, TimeUnit.MILLISECONDS);
            if (snapshotShard != null) {
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() {
                        if (isMaster == false) {
                            logger.debug("skipping snapshot shard size retrieval for {} as node is no longer master", snapshotShard);
                            return;
                        }

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
                        synchronized (InternalSnapshotsInfoService.this) {
                            if (isMaster) {
                                final ImmutableOpenMap.Builder<SnapshotShard, Long> newSnapshotShardSizes =
                                    ImmutableOpenMap.builder(knownSnapshotShardSizes);
                                updated = newSnapshotShardSizes.put(snapshotShard, snapshotShardSize) == null;
                                assert updated : "snapshot shard size already exists for " + snapshotShard;
                                knownSnapshotShardSizes = newSnapshotShardSizes.build();
                            }
                        }
                        if (updated) {
                            rerouteService.get().reroute("snapshot shard size updated", Priority.HIGH, REROUTE_LISTENER);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(() -> new ParameterizedMessage("failed to retrieve shard size for {}", snapshotShard), e);
                    }

                    @Override
                    public void onAfter() {
                        unknownSnapshotShards.remove(snapshotShard);
                        fetchNextSnapshotShard();
                    }
                });
                success = true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("snapshot shard size fetcher has been interrupted", e);
        } finally {
            if (success == false) {
                final int value = concurrentFetches.decrementAndGet();
                assert value >= 0 : "Unexpected value: " + value;
            }
        }
    }

    // used in tests
    int numberOfUnknownSnapshotShardSizes() {
        return unknownSnapshotShards.size();
    }

    // used in tests
    int numberOfKnownSnapshotShardSizes() {
        return knownSnapshotShardSizes.size();
    }

    public static class SnapshotShard {

        private final Snapshot snapshot;
        private final IndexId index;
        private final ShardId shardId;

        public SnapshotShard(Snapshot snapshot, IndexId index, ShardId shardId) {
            this.snapshot = snapshot;
            this.index = index;
            this.shardId = shardId;
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        public IndexId index() {
            return index;
        }

        public ShardId shardId() {
            return shardId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final SnapshotShard that = (SnapshotShard) o;
            return shardId.equals(that.shardId)
                && snapshot.equals(that.snapshot)
                && index.equals(that.index);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshot, index, shardId);
        }

        @Override
        public String toString() {
            return "[" +
                "snapshot=" + snapshot +
                ", index=" + index +
                ", shard=" + shardId +
                ']';
        }
    }
}
