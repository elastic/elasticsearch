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

package org.elasticsearch.snapshots;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.Version;

/**
 * Represent information about snapshot
 */
public interface Snapshot extends Comparable<Snapshot> {
    /**
     * Returns snapshot name
     *
     * @return snapshot name
     */
    String name();

    /**
     * Returns current snapshot state
     *
     * @return snapshot state
     */
    SnapshotState state();

    /**
     * Returns reason for complete snapshot failure
     *
     * @return snapshot failure reason
     */
    String reason();

    /**
     * Returns version of Elasticsearch that was used to create this snapshot
     *
     * @return Elasticsearch version
     */
    Version version();

    /**
     * Returns indices that were included into this snapshot
     *
     * @return list of indices
     */
    ImmutableList<String> indices();

    /**
     * Returns time when snapshot started
     *
     * @return snapshot start time
     */
    long startTime();

    /**
     * Returns time when snapshot ended
     * <p/>
     * Can be 0L if snapshot is still running
     *
     * @return snapshot end time
     */
    long endTime();

    /**
     * Returns total number of shards that were snapshotted
     *
     * @return number of shards
     */
    int totalShard();

    /**
     * Returns total number of shards that were successfully snapshotted
     *
     * @return number of successful shards
     */
    int successfulShards();

    /**
     * Returns shard failures
     *
     * @return shard failures
     */
    ImmutableList<SnapshotShardFailure> shardFailures();

}
