/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * <p>This package exposes the Elasticsearch Snapshot functionality.</p>
 *
 * <h2>Preliminaries</h2>
 *
 * <p>There are two communication channels between all nodes and master in the snapshot functionality:</p>
 * <ul>
 * <li>The master updates the cluster state by adding, removing or altering the contents of its custom entry
 * {@link org.elasticsearch.cluster.SnapshotsInProgress}. All nodes consume the state of the {@code SnapshotsInProgress} and will start or
 * abort relevant shard snapshot tasks accordingly.</li>
 * <li>Nodes that are executing shard snapshot tasks report either success or failure of their snapshot task by submitting a
 * {@link org.elasticsearch.snapshots.UpdateIndexShardSnapshotStatusRequest} to the master node that will update the
 * snapshot's entry in the cluster state accordingly.</li>
 * </ul>
 *
 * <h2>Snapshot Creation</h2>
 * <p>Snapshots are created by the following sequence of events:</p>
 * <ol>
 * <li>First the {@link org.elasticsearch.snapshots.SnapshotsService} determines the primary shards' assignments for all indices that are
 * being snapshotted and creates a {@code SnapshotsInProgress.Entry} with state {@code STARTED} and adds the map of
 * {@link org.elasticsearch.index.shard.ShardId} to {@link org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus} that tracks
 * the assignment of which node is to snapshot which shard. All shard snapshots are executed on the shard's primary node. Thus all shards
 * for which the primary node was found to have a healthy copy of the shard are marked as being in state {@code INIT} in this map. If the
 * primary for a shard is unassigned, it is marked as {@code MISSING} in this map. In case the primary is initializing at this point, it is
 * marked as in state {@code WAITING}. In case a shard's primary is relocated at any point after its {@code SnapshotsInProgress.Entry} was
 * created and thus been assigned to a specific cluster node, that shard's snapshot will fail and move to state {@code FAILED}.</li>
 *
 * <li>The new {@code SnapshotsInProgress.Entry} is then observed by
 * {@link org.elasticsearch.snapshots.SnapshotShardsService#clusterChanged} on all nodes and since the entry is in state {@code STARTED}
 * the {@code SnapshotShardsService} will check if any local primary shards are to be snapshotted (signaled by the shard's snapshot state
 * being {@code INIT}). For those local primary shards found in state {@code INIT}) the snapshot process of writing the shard's data files
 * to the snapshot's {@link org.elasticsearch.repositories.Repository} is executed. Once the snapshot execution finishes for a shard an
 * {@code UpdateIndexShardSnapshotStatusRequest} is sent to the master node signaling either status {@code SUCCESS} or {@code FAILED}.
 * The master node will then update a shard's state in the snapshots {@code SnapshotsInProgress.Entry} whenever it receives such a
 * {@code UpdateIndexShardSnapshotStatusRequest}.</li>
 *
 * <li>If as a result of the received status update requests, all shards in the cluster state are in a completed state, i.e are marked as
 * either {@code SUCCESS}, {@code FAILED} or {@code MISSING}, the {@code SnapshotShardsService} will update the state of the {@code Entry}
 * itself and mark it as {@code SUCCESS}. At the same time {@link org.elasticsearch.snapshots.SnapshotsService#endSnapshot} is executed,
 * writing the metadata necessary to finalize the snapshot in the repository to the repository.</li>
 *
 * <li>After writing the final metadata to the repository, a cluster state update to remove the snapshot from the cluster state is
 * submitted and the removal of the snapshot's {@code SnapshotsInProgress.Entry} from the cluster state completes the snapshot process.
 * </li>
 * </ol>
 *
 * <h2>Deleting a Snapshot</h2>
 *
 * <p>Deleting a snapshot can take the form of either simply deleting it from the repository or (if it has not completed yet) aborting it
 * and subsequently deleting it from the repository.</p>
 *
 * <h2>Aborting a Snapshot</h2>
 *
 * <ol>
 * <li>Aborting a snapshot starts by updating the state of the snapshot's {@code SnapshotsInProgress.Entry} to {@code ABORTED}.</li>
 *
 * <li>The snapshot's state change to {@code ABORTED} in cluster state is then picked up by the {@code SnapshotShardsService} on all nodes.
 * Those nodes that have shard snapshot actions for the snapshot assigned to them, will abort them and notify master about the shards
 * snapshot status accordingly. If the shard snapshot action completed or was in state {@code FINALIZE} when the abort was registered by
 * the {@code SnapshotShardsService}, then the shard's state will be reported to master as {@code SUCCESS}.
 * Otherwise, it will be reported as {@code FAILED}.</li>
 *
 * <li>Once all the shards are reported to master as either {@code SUCCESS} or {@code FAILED} the {@code SnapshotsService} on the master
 * will finish the snapshot process as all shard's states are now completed and hence the snapshot can be completed as explained in point 4
 * of the snapshot creation section above.</li>
 * </ol>
 *
 * <h2>Deleting a Snapshot from a Repository</h2>
 *
 * <ol>
 * <li>Assuming there are no entries in the cluster state's {@code SnapshotsInProgress}, deleting a snapshot starts by the
 * {@code SnapshotsService} creating an entry for deleting the snapshot in the cluster state's
 * {@link org.elasticsearch.cluster.SnapshotDeletionsInProgress}.</li>
 *
 * <li>Once the cluster state contains the deletion entry in {@code SnapshotDeletionsInProgress} the {@code SnapshotsService} will invoke
 * {@link org.elasticsearch.repositories.Repository#deleteSnapshots} for the given snapshot, which will remove files associated with the
 * snapshot from the repository as well as update its meta-data to reflect the deletion of the snapshot.</li>
 *
 * <li>After the deletion of the snapshot's data from the repository finishes, the {@code SnapshotsService} will submit a cluster state
 * update to remove the deletion's entry in {@code SnapshotDeletionsInProgress} which concludes the process of deleting a snapshot.</li>
 * </ol>
 */
package org.elasticsearch.snapshots;
