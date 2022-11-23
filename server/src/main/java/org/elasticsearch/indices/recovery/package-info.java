/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * <p>This package contains the logic for the recovery functionality.</p>
 *
 * <h2>Preliminaries</h2>
 *
 * Recoveries are started on data nodes as a result of data node discovering shard assignments to themselves in the cluster state. The
 * master node sets up these shard allocations in the cluster state (see {@link org.elasticsearch.cluster.routing.ShardRouting}).
 * If a data node finds shard allocations that require recovery on itself, it will execute the required recoveries by executing the
 * logic starting at {@link org.elasticsearch.indices.cluster.IndicesClusterStateService#createIndicesAndUpdateShards}. As the data nodes
 * execute the steps of the recovery state machine they report back success or failure to do so to the master node via the transport
 * actions in {@link org.elasticsearch.cluster.action.shard.ShardStateAction}, which will then update the shard routing in the cluster state
 * accordingly to reflect the status of the recovered shards or to handle failures in the recovery process. Recoveries can have various
 * kinds of sources that are modeled via the {@link org.elasticsearch.cluster.routing.RecoverySource} that is communicated to the recovery
 * target by {@link org.elasticsearch.cluster.routing.ShardRouting#recoverySource()} for each shard routing. These sources and their state
 * machines will be described below. The actual recovery process for all of them is started by invoking
 * {@link org.elasticsearch.index.shard.IndexShard#startRecovery}.
 *
 * <h3>Checkpoints</h3>
 *
 * Aspects of the recovery logic are based on the concepts of local and global checkpoints. Each operation on a shard is tracked by a
 * sequence number as well as the primary term during which it was applied to the index. The sequence number up to which operations have
 * been fully processed on a shard is that shard's local checkpoint. The sequence number up to which operations on all replicas for a shard
 * have been fully processed is referred to as the global checkpoint. Comparing the local checkpoints of shard copies enables determining
 * which operations would have to be replayed to a shard copy to bring it in-sync with the primary shard. By retaining operations in the
 * {@link org.elasticsearch.indices.recovery.RecoveryState.Translog} or in soft deletes, they are available for this kind of replay that
 * moves a shard lower local checkpoint up to a higher local checkpoint. The global checkpoint allows for determining which operations have
 * been safely processed on all shards and thus don't have to be retained on the primary node for replay to replicas.
 *
 * The primary node tracks the global checkpoint for a shard via the {@link org.elasticsearch.index.seqno.ReplicationTracker}. The primary
 * term is tracked by the master node and stored in the cluster state and incremented each time the primary node for a shard changes.
 *
 * <h3>Retention Leases</h3>
 *
 * The duration for which a shard retains individual operations for replay during recovery is governed by the
 * {@link org.elasticsearch.index.seqno.RetentionLease} functionality. More information about this functionality can be found in the
 * {@link org.elasticsearch.index.seqno} package and the "History retention" section in the docs.
 *
 * <h2>Recovery Types</h2>
 *
 * <h3>1. Peer Recovery</h3>
 *
 * Peer recovery is the process of bringing a shard copy on one node, referred to as the target node below, in-sync with the shard copy on
 * another node, referred to as the source node below. It is always the primary node of a shard that serves as the source of the recovery.
 * On a high level, recovery happens by a combination of comparing and subsequently synchronizing files and operations from the source to
 * the target.
 * Synchronizing the on-disk file structure on the target with those on the source node is referred to as file-based recovery.
 * Synchronizing operations based on comparing checkpoints is commonly referred to as ops-based recovery. As primaries and replicas are
 * independent Lucene indices that will execute their Lucene level merges independently the concrete on-disk file structure on a pair of
 * primary and replica nodes for a given shard will diverge over time even if both copies of the shard hold the exact same set of documents
 * and operations. Peer recovery will therefore try to avoid file-based recovery where possible to reduce the amount of data that has to be
 * transferred. It will prefer replaying just those operations missing on the target relative to the source instead as this
 * avoids copying files from source to target that could contain data that is for the most part already present on the target.
 * Replaying operations is possible as long as the primary node retains the missing operations as soft-deletes in its Lucene index.
 *
 * <h4>State Machine</h4>
 *
 * Peer recoveries are modeled via a {@link org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource}. They start by moving the
 * shard's state to {@link org.elasticsearch.index.shard.IndexShardState#RECOVERING} and then triggering the peer recovery through a call
 * to {@link org.elasticsearch.indices.recovery.PeerRecoveryTargetService#startRecovery} which results in the following steps being
 * executed.
 *
 * <ol>
 *     <li>
 *         The target shard starts out with a {@link org.elasticsearch.indices.recovery.RecoveryState} at stage
 *         {@link org.elasticsearch.indices.recovery.RecoveryState.Stage#INIT}. At the start of the peer recovery process, the target node
 *         will try to recover from its local translog as far as if there are any operations to recover from it. It will first move to
 *         stage {@link org.elasticsearch.indices.recovery.RecoveryState.Stage#INDEX} and then try to recover as far as possible from
 *         existing files and the existing translog. During this process, it will move to
 *         {@link org.elasticsearch.indices.recovery.RecoveryState.Stage#VERIFY_INDEX}, verifying that the files on disk are not corrupted,
 *         then to {@link org.elasticsearch.indices.recovery.RecoveryState.Stage#TRANSLOG} during recovery from translog.
 *         A {@link  org.elasticsearch.indices.recovery.StartRecoveryRequest} is then sent to the primary node of the shard to recover by
 *         the target node for the recovery. This triggers
 *         {@link org.elasticsearch.indices.recovery.PeerRecoverySourceService#recover} on the primary node that receives the request. The
 *         {@code StartRecoveryRequest} contains information about the local state of the recovery target, based on which the recovery
 *         source will determine the recovery mechanism (file-based or ops-based) to use.
 *     </li>
 *     <li>
 *        When determining whether to use ops-based recovery the recovery source will check the following conditions
 *        that must all be true simultaneously for ops-based recovery to be executed:
 *        <ul>
 *            <li>
 *                Target shard and source shard must share the same
 *                {@link org.elasticsearch.index.engine.Engine#HISTORY_UUID_KEY} in their latest Lucene commit.
 *            </li>
 *            <li>
 *                The source must have retained all operations between the latest sequence number present on the target.
 *                See {@link org.elasticsearch.index.shard.IndexShard#hasCompleteHistoryOperations} for details.
 *            </li>
 *            <li>
 *                A peer recovery retention lease must exist for the target shard and it must retain a sequence number below or equal
 *                to the starting sequence number in {@link org.elasticsearch.indices.recovery.StartRecoveryRequest#startingSeqNo()}.
 *            </li>
 *        </ul>
 *     </li>
 *     <li>
 *         In case the preconditions for ops-based recovery aren't met, file-based recovery is executed first.
 *         To trigger file-based recovery, the source node will execute phase 1 of the recovery by invoking
 *         {@link org.elasticsearch.indices.recovery.RecoverySourceHandler#phase1}. Using the information about the files on the target node
 *         found in the {@code StartRecoveryRequest}, phase 1 will determine what segment files must be copied to the recovery target.
 *         The information about these files will then be sent to the recovery target via a
 *         {@link org.elasticsearch.indices.recovery.RecoveryFilesInfoRequest}. Once the recovery target has received the list of files
 *         that will be copied to it, {@link org.elasticsearch.indices.recovery.RecoverySourceHandler#sendFiles} is invoked which
 *         will send the segment files over to the recovery target via a series of
 *         {@link org.elasticsearch.indices.recovery.RecoveryFileChunkRequest}.
 *         Receiving a {@code RecoveryFilesInfoRequest} on the target indicates to it that the recovery will be file-based so it will
 *         invoke {@link org.elasticsearch.index.shard.IndexShard#resetRecoveryStage} to reset the recovery back to {@code INIT} stage and
 *         then prepare for receiving files and move to stage {@code INDEX} again.
 *     </li>
 *     <li>
 *         Once all the file chunks have been received by the recovery target, a retention lease for the latest global checkpoint is
 *         created by the source node to ensure all remaining operations from the latest global checkpoint are retained for replay in
 *         the next step of the recovery. Also, after creating the retention lease and before moving on to the next step of the peer
 *         recovery process, a {@link org.elasticsearch.indices.recovery.RecoveryCleanFilesRequest} is sent from the source to the target.
 *         The target will handle this request by doing the following:
 *         <ul>
 *             <li>
 *                 The file chunks from the previous step were saved to temporary file names. They are now renamed to their original
 *                 names.
 *             </li>
 *             <li>
 *                 Cleanup all files in the shard directory that are not part of the recovering shard copy.
 *             </li>
 *             <li>
 *                 Trigger creation of a new translog on the target. This moves the recovery stage on the target to
 *                 {@link org.elasticsearch.indices.recovery.RecoveryState.Stage#TRANSLOG}.
 *             </li>
 *         </ul>
 *     </li>
 *     <li>
 *         After the segment files synchronization from source to the target has finished or was skipped, the translog based recovery
 *         step is executed by invoking {@link org.elasticsearch.indices.recovery.RecoverySourceHandler#prepareTargetForTranslog} on the
 *         recovery source. This sends a {@link org.elasticsearch.indices.recovery.RecoveryPrepareForTranslogOperationsRequest} to the
 *         recovery target which contains the estimated number of translog operations that have to be copied to the target.
 *         On the target, this request is handled and triggers a call to
 *         {@link org.elasticsearch.index.shard.IndexShard#openEngineAndSkipTranslogRecovery()} which opens a new engine and translog
 *         and then responds back to the recovery source.
 *         Once the recovery source receives that response, it invokes
 *         {@link org.elasticsearch.indices.recovery.RecoverySourceHandler#phase2} to replay outstanding translog operations on the target.
 *         This is done by sending a series of {@link org.elasticsearch.indices.recovery.RecoveryTranslogOperationsRequest} to the target
 *         which will respond with {@link org.elasticsearch.indices.recovery.RecoveryTranslogOperationsResponse}s which contain the
 *         maximum persisted local checkpoint for the target. Tracking the maximum of the received local checkpoint values is necessary
 *         for the next step, finalizing the recovery.
 *     </li>
 *     <li>
 *         After replaying the translog operations on the target, the recovery is finalized by a call to
 *         {@link org.elasticsearch.indices.recovery.RecoverySourceHandler#finalizeRecovery} on the source. With the knowledge that the
 *         target has received all operations up to the maximum local checkpoint tracked in the previous step, the source
 *         (which is also the primary) can now update its in-sync checkpoint state by a call to
 *         {@link org.elasticsearch.index.seqno.ReplicationTracker#markAllocationIdAsInSync}.
 *         Once the in-sync sequence number information has been persisted successfully, the source sends a
 *         {@link org.elasticsearch.indices.recovery.RecoveryFinalizeRecoveryRequest} to the target which contains the global checkpoint
 *         as well as a sequence number above which the target can trim all operations from its translog since all operations above this
 *         number have just been replayed in the previous step and were either of the same or a newer version that those in the existing
 *         translog on the target. This step then also moves the target to the recovery stage
 *         {@link org.elasticsearch.indices.recovery.RecoveryState.Stage#FINALIZE}.
 *     </li>
 *     <li>
 *         After the finalization step, the recovery source will send a {@link org.elasticsearch.indices.recovery.RecoveryResponse} to the
 *         target which is implemented as a response to the initial {@code StartRecoveryRequest} that the target sent to initiate the
 *         recovery. This leads to a call to {@link org.elasticsearch.index.shard.IndexShard#postRecovery} which moves the recovery state
 *         to stage {@link org.elasticsearch.indices.recovery.RecoveryState.Stage#DONE}, triggers a refresh of the shard and moves the
 *         shard to state {@link org.elasticsearch.index.shard.IndexShardState#POST_RECOVERY}. Finally, the recovery target will then
 *         send a {@link org.elasticsearch.cluster.action.shard.ShardStateAction.StartedShardEntry} transport message to master to inform
 *         it about the successful start of the shard.
 *     </li>
 *     <li>
 *         After receiving the {@code StartedShardEntry}, master will then update the cluster state to reflect the state of the now fully
 *         recovered recovery target by executing the
 *         {@link org.elasticsearch.cluster.action.shard.ShardStateAction.ShardStartedClusterStateTaskExecutor}. The resulting cluster
 *         state update is then observed by {@link org.elasticsearch.index.shard.IndexShard#updateShardState} which updates the shard state
 *         on the target node to {@link org.elasticsearch.index.shard.IndexShardState#STARTED} thus completing the peer recovery.
 *     </li>
 * </ol>
 *
 * TODO: document other recovery types
 */
package org.elasticsearch.indices.recovery;
