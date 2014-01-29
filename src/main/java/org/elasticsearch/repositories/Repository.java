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
package org.elasticsearch.repositories;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotShardFailure;

/**
 * Snapshot repository interface.
 * <p/>
 * Responsible for index and cluster level operations. It's called only on master.
 * Shard-level operations are performed using {@link org.elasticsearch.index.snapshots.IndexShardRepository}
 * interface on data nodes.
 * <p/>
 * Typical snapshot usage pattern:
 * <ul>
 * <li>Master calls {@link #initializeSnapshot(org.elasticsearch.cluster.metadata.SnapshotId, com.google.common.collect.ImmutableList, org.elasticsearch.cluster.metadata.MetaData)}
 * with list of indices that will be included into the snapshot</li>
 * <li>Data nodes call {@link org.elasticsearch.index.snapshots.IndexShardRepository#snapshot(org.elasticsearch.cluster.metadata.SnapshotId, org.elasticsearch.index.shard.ShardId, org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit, org.elasticsearch.index.snapshots.IndexShardSnapshotStatus)} for each shard</li>
 * <li>When all shard calls return master calls {@link #finalizeSnapshot(org.elasticsearch.cluster.metadata.SnapshotId, String, int, com.google.common.collect.ImmutableList)}
 * with possible list of failures</li>
 * </ul>
 */
public interface Repository extends LifecycleComponent<Repository> {

    /**
     * Reads snapshot description from repository.
     *
     * @param snapshotId snapshot ID
     * @return information about snapshot
     */
    Snapshot readSnapshot(SnapshotId snapshotId);

    /**
     * Returns global metadata associate with the snapshot.
     * <p/>
     * The returned meta data contains global metadata as well as metadata for all indices listed in the indices parameter.
     *
     * @param snapshotId snapshot ID
     * @param indices    list of indices
     * @return information about snapshot
     */
    MetaData readSnapshotMetaData(SnapshotId snapshotId, ImmutableList<String> indices);

    /**
     * Returns the list of snapshots currently stored in the repository
     *
     * @return snapshot list
     */
    ImmutableList<SnapshotId> snapshots();

    /**
     * Starts snapshotting process
     *
     * @param snapshotId snapshot id
     * @param indices    list of indices to be snapshotted
     * @param metaData   cluster metadata
     */
    void initializeSnapshot(SnapshotId snapshotId, ImmutableList<String> indices, MetaData metaData);

    /**
     * Finalizes snapshotting process
     * <p/>
     * This method is called on master after all shards are snapshotted.
     *
     * @param snapshotId    snapshot id
     * @param failure       global failure reason or null
     * @param totalShards   total number of shards
     * @param shardFailures list of shard failures
     * @return snapshot description
     */
    Snapshot finalizeSnapshot(SnapshotId snapshotId, String failure, int totalShards, ImmutableList<SnapshotShardFailure> shardFailures);

    /**
     * Deletes snapshot
     *
     * @param snapshotId snapshot id
     */
    void deleteSnapshot(SnapshotId snapshotId);

    /**
     * Returns snapshot throttle time in nanoseconds
     */
    long snapshotThrottleTimeInNanos();

    /**
     * Returns restore throttle time in nanoseconds
     */
    long restoreThrottleTimeInNanos();


}
