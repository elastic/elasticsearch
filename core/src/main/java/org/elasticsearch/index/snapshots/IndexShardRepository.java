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

package org.elasticsearch.index.snapshots;

import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;

/**
 * Shard-level snapshot repository
 * <p/>
 * IndexShardRepository is used on data node to create snapshots of individual shards. See {@link org.elasticsearch.repositories.Repository}
 * for more information.
 */
public interface IndexShardRepository {

    /**
     * Creates a snapshot of the shard based on the index commit point.
     * <p/>
     * The index commit point can be obtained by using {@link org.elasticsearch.index.engine.Engine#snapshotIndex()} method.
     * IndexShardRepository implementations shouldn't release the snapshot index commit point. It is done by the method caller.
     * <p/>
     * As snapshot process progresses, implementation of this method should update {@link IndexShardSnapshotStatus} object and check
     * {@link IndexShardSnapshotStatus#aborted()} to see if the snapshot process should be aborted.
     *
     * @param snapshotId          snapshot id
     * @param shardId             shard to be snapshotted
     * @param snapshotIndexCommit commit point
     * @param snapshotStatus      snapshot status
     */
    void snapshot(SnapshotId snapshotId, ShardId shardId, SnapshotIndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus);

    /**
     * Restores snapshot of the shard.
     * <p/>
     * The index can be renamed on restore, hence different {@code shardId} and {@code snapshotShardId} are supplied.
     *
     * @param snapshotId      snapshot id
     * @param shardId         shard id (in the current index)
     * @param snapshotShardId shard id (in the snapshot)
     * @param recoveryState   recovery state
     */
    void restore(SnapshotId snapshotId, ShardId shardId, ShardId snapshotShardId, RecoveryState recoveryState);

    /**
     * Retrieve shard snapshot status for the stored snapshot
     *
     * @param snapshotId snapshot id
     * @param shardId    shard id
     * @return snapshot status
     */
    IndexShardSnapshotStatus snapshotStatus(SnapshotId snapshotId, ShardId shardId);

    /**
     * Verifies repository settings on data node
     * @param verificationToken value returned by {@link org.elasticsearch.repositories.Repository#startVerification()}
     */
    void verify(String verificationToken);

}
