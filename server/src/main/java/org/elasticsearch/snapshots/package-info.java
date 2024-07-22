/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
 * either {@code SUCCESS}, {@code FAILED} or {@code MISSING}, the {@code SnapshotsService} will update the state of the {@code Entry}
 * itself and mark it as {@code SUCCESS}. At the same time {@link org.elasticsearch.snapshots.SnapshotsService#endSnapshot} is executed,
 * writing to the repository the metadata necessary to finalize the snapshot in the repository.</li>
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
 *
 * <h2>Cloning a Snapshot</h2>
 *
 * <p>Cloning part of a snapshot is a process executed entirely on the master node. On a high level, the process of cloning a snapshot is
 * analogous to that of creating a snapshot from data in the cluster except that the source of data files is the snapshot repository
 * instead of the data nodes. It begins with cloning all shards and then finalizes the cloned snapshot the same way a normal snapshot would
 * be finalized. Concretely, it is executed as follows:</p>
 *
 * <ol>
 *     <li>First, {@link org.elasticsearch.snapshots.SnapshotsService#cloneSnapshot} is invoked which will place a placeholder entry into
 *     {@code SnapshotsInProgress} that does not yet contain any shard clone assignments. Note that unlike in the case of snapshot
 *     creation, the shard level clone tasks in
 *     {@link org.elasticsearch.cluster.SnapshotsInProgress.Entry#shardSnapshotStatusByRepoShardId()} are not created in the initial cluster
 *     state update as is done for shard snapshot assignments in {@link org.elasticsearch.cluster.SnapshotsInProgress.Entry#shards}. This is
 *     due to the fact that shard snapshot assignments are computed purely from information in the current cluster state while shard clone
 *     assignments require information to be read from the repository, which is too slow of a process to be done inside a cluster state
 *     update. Loading this information ahead of creating a task in the cluster state, runs the risk of race conditions where the source
 *     snapshot is being deleted before the clone task is enqueued in the cluster state.</li>
 *     <li>Once a placeholder task for the clone operation is put into the cluster state, we must determine the number of shards in each
 *     index that is to be cloned as well as ensure the health of the index snapshots in the source snapshot. In order to determine the
 *     shard count for each index that is to be cloned, we load the index metadata for each such index using the repository's
 *     {@link org.elasticsearch.repositories.Repository#getSnapshotIndexMetaData} method. In order to ensure the health of the source index
 *     snapshots, we load the {@link org.elasticsearch.snapshots.SnapshotInfo} for the source snapshot and check for shard snapshot
 *     failures of the relevant indices.</li>
 *     <li>Once all shard counts are known and the health of all source indices data has been verified, we populate the
 *     {@code SnapshotsInProgress.Entry#clones} map for the clone operation with the the relevant shard clone tasks.</li>
 *     <li>After the clone tasks have been added to the {@code SnapshotsInProgress.Entry}, master executes them on its snapshot thread-pool
 *     by invoking {@link org.elasticsearch.repositories.Repository#cloneShardSnapshot} for each shard that is to be cloned. Each completed
 *     shard snapshot triggers a call to the {@link org.elasticsearch.snapshots.SnapshotsService#masterServiceTaskQueue} which updates the
 *     clone's {@code SnapshotsInProgress.Entry} to mark the shard clone operation completed.</li>
 *     <li>Once all the entries in {@code SnapshotsInProgress.Entry#clones} have completed, the clone is finalized just like any other
 *     snapshot through {@link org.elasticsearch.snapshots.SnapshotsService#endSnapshot}. The only difference being that the metadata that
 *     is written out for indices and the global metadata are read from the source snapshot in the repository instead of the cluster state.
 *     </li>
 * </ol>
 *
 * <h2>Concurrent Snapshot Operations</h2>
 *
 * Snapshot create and delete operations may be started concurrently. Operations targeting different repositories run independently of
 * each other. Multiple operations targeting the same repository are executed according to the following rules:
 *
 * <h3>Concurrent Snapshot Creation</h3>
 *
 * If multiple snapshot creation jobs are started at the same time, the data-node operations of multiple snapshots may run in parallel
 * across different shards. If multiple snapshots want to snapshot a certain shard, then the shard snapshots for that shard will be
 * executed one by one. This is enforced by the master node setting the shard's snapshot state to
 * {@link org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus#UNASSIGNED_QUEUED} for all but one snapshot. The order of
 * operations on a single shard is given by the order in which the snapshots were started.
 * As soon as all shards for a given snapshot have finished, it will be finalized as explained above. Finalization will happen one snapshot
 * at a time, working in the order in which snapshots had their shards completed.
 *
 * <h3>Concurrent Snapshot Deletes</h3>
 *
 * A snapshot delete will be executed as soon as there are no more shard snapshots or snapshot finalizations executing running for a given
 * repository. Before a delete is executed on the repository it will be set to state
 * {@link org.elasticsearch.cluster.SnapshotDeletionsInProgress.State#STARTED}. If it cannot be executed when it is received it will be
 * set to state {@link org.elasticsearch.cluster.SnapshotDeletionsInProgress.State#WAITING} initially.
 * If a delete is received for a given repository while there is already an ongoing delete for the same repository, there are two possible
 * scenarios:
 * 1. If the delete is in state {@code META_DATA} (i.e. already running on the repository) then the new delete will be added in state
 * {@code WAITING} and will be executed after the current delete. The only exception here would be the case where the new delete covers
 * the exact same snapshots as the already running delete. In this case no new delete operation is added and second delete request will
 * simply wait for the existing delete to return.
 * 2. If the existing delete is in state {@code WAITING} then the existing
 * {@link org.elasticsearch.cluster.SnapshotDeletionsInProgress.Entry} in the cluster state will be updated to cover both the snapshots
 * in the existing delete as well as additional snapshots that may be found in the second delete request.
 *
 * In either of the above scenarios, in-progress snapshots will be aborted in the same cluster state update that adds a delete to the
 * cluster state, if a delete applies to them.
 *
 * If a snapshot request is received while there already is a delete in the cluster state for the same repository, that snapshot will not
 * start doing any shard snapshots until the delete has been executed.
 */
package org.elasticsearch.snapshots;
