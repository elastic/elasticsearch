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
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public class InternalSnapshotsInfoService implements ClusterStateListener, SnapshotsInfoService {

    private static final Logger logger = LogManager.getLogger(InternalSnapshotsInfoService.class);

    private final ThreadPool threadPool;
    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final Supplier<RerouteService> rerouteServiceSupplier;

    private volatile ImmutableOpenMap<SnapshotShard, Long> snapshotShardSizes;

    private final Set<SnapshotShard> fetchingShards = Sets.newConcurrentHashSet();

    public InternalSnapshotsInfoService(ThreadPool threadPool, Supplier<RepositoriesService> repositoriesServiceSupplier,
                                        Supplier<RerouteService> rerouteServiceSupplier) {
        this.threadPool = threadPool;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.rerouteServiceSupplier = rerouteServiceSupplier;
        snapshotShardSizes = ImmutableOpenMap.of();
    }

    public static class SnapshotShard {

        private final Snapshot snapshot;
        private final IndexId index;
        private final int shardId;

        public SnapshotShard(Snapshot snapshot, IndexId index, int shardId) {
            this.snapshot = snapshot;
            this.index = index;
            this.shardId = shardId;
        }

        public Snapshot getSnapshot() {
            return snapshot;
        }

        public IndexId getIndex() {
            return index;
        }

        public int getShardId() {
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
            return shardId == that.shardId
                && snapshot.equals(that.snapshot)
                && index.equals(that.index);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshot, index, shardId);
        }

        @Override
        public String toString() {
            return "SnapshotShard{" +
                "snapshot=" + snapshot +
                ", index=" + index +
                ", shardId=" + shardId +
                '}';
        }
    }

    @Override
    public SnapshotShardSizeInfo snapshotShardSizes() {
        return new SnapshotShardSizeInfo(snapshotShardSizes);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            final Set<SnapshotShard> requiredSnapshotShards = new HashSet<>();
            for (ShardRouting shardRouting : event.state().routingTable().shardsWithState(ShardRoutingState.UNASSIGNED)) {
                if (shardRouting.primary() && shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                    final RecoverySource.SnapshotRecoverySource snapshotRecoverySource =
                        (RecoverySource.SnapshotRecoverySource) shardRouting.recoverySource();
                    final SnapshotShard snapshotShard = new SnapshotShard(snapshotRecoverySource.snapshot(),
                        snapshotRecoverySource.index(), shardRouting.id());
                    requiredSnapshotShards.add(snapshotShard);
                    // check if already populated entry
                    if (snapshotShardSizes.get(snapshotShard) == null) {
                        // check if already fetching snapshot info in progress
                        if (fetchingShards.add(snapshotShard)) {
                            // TODO: Use a dedicated threadpool here? Use FETCH thread pool?
                            threadPool.generic().execute(new AbstractRunnable() {
                                @Override
                                public void onFailure(Exception e) {
                                    logger.warn(new ParameterizedMessage("failed to retrieve shard size information for {}",
                                        shardRouting), e);
                                }

                                @Override
                                protected void doRun() {
                                    final RepositoriesService repositories = repositoriesServiceSupplier.get();
                                    assert repositories != null;
                                    final Repository repository = repositories.repository(snapshotShard.snapshot.getRepository());
                                    final IndexShardSnapshotStatus status =
                                        repository.getShardSnapshotStatus(snapshotRecoverySource.snapshot().getSnapshotId(),
                                        snapshotRecoverySource.index(), shardRouting.shardId());
                                    final long snapshotShardSize = status.asCopy().getTotalSize();
                                    boolean updated;
                                    synchronized (InternalSnapshotsInfoService.this) {
                                        final ImmutableOpenMap.Builder<SnapshotShard, Long> newSnapshotShardSizes =
                                            ImmutableOpenMap.builder(snapshotShardSizes);
                                        updated = newSnapshotShardSizes.put(snapshotShard, snapshotShardSize) == null;
                                        snapshotShardSizes = newSnapshotShardSizes.build();
                                    }
                                    if (updated) {
                                        rerouteServiceSupplier.get().reroute("snapshot shard size updated", Priority.HIGH,
                                            ActionListener.wrap(
                                                r -> logger.trace("reroute after snapshot shard size update completed"),
                                                e -> logger.debug("reroute after snapshot shard size update failed", e)));
                                    }
                                }

                                @Override
                                public void onAfter() {
                                    fetchingShards.remove(snapshotShard);
                                }
                            });
                        }
                    }
                }
            }
            // Clean up keys from snapshotShardSizes that are no longer needed for recoveries
            synchronized (this) {
                ImmutableOpenMap.Builder<SnapshotShard, Long> newSnapshotShardSizes = null;
                for (ObjectCursor<SnapshotShard> shard : snapshotShardSizes.keys()) {
                    if (requiredSnapshotShards.contains(shard.value) == false) {
                        if (newSnapshotShardSizes == null) {
                            newSnapshotShardSizes = ImmutableOpenMap.builder(snapshotShardSizes);
                        }
                        newSnapshotShardSizes.remove(shard.value);
                    }
                }
                if (newSnapshotShardSizes != null) {
                    snapshotShardSizes = newSnapshotShardSizes.build();
                }
            }
        } else {
            // information only needed on current master
            snapshotShardSizes = ImmutableOpenMap.of();
        }
    }

}
