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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.io.Serializable;

/**
 * Shard routing represents the state of a shard instance allocated in the cluster.
 */
public interface ShardRouting extends Streamable, Serializable, ToXContent {

    /**
     * The shard id.
     */
    ShardId shardId();

    /**
     * The index name.
     */
    String index();

    /**
     * The index name.
     */
    String getIndex();

    /**
     * The shard id.
     */
    int id();

    /**
     * The shard id.
     */
    int getId();

    /**
     * The routing version associated with the shard.
     */
    long version();

    /**
     * The shard state.
     */
    ShardRoutingState state();

    /**
     * The shard is unassigned (not allocated to any node).
     */
    boolean unassigned();

    /**
     * The shard is initializing (usually recovering either from peer shard
     * or from gateway).
     */
    boolean initializing();

    /**
     * The shard is in started mode.
     */
    boolean started();

    /**
     * Returns <code>true</code> iff the this shard is currently relocating to
     * another node. Otherwise <code>false</code>
     *
     * @see ShardRoutingState#RELOCATING
     */
    boolean relocating();

    /**
     * Returns <code>true</code> iff the this shard is currently
     * {@link ShardRoutingState#STARTED started} or
     * {@link ShardRoutingState#RELOCATING relocating} to another node.
     * Otherwise <code>false</code>
     */
    boolean active();

    /**
     * Returns <code>true</code> iff this shard is assigned to a node ie. not
     * {@link ShardRoutingState#UNASSIGNED unassigned}. Otherwise <code>false</code>
     */
    boolean assignedToNode();

    /**
     * The current node id the shard is allocated on.
     */
    String currentNodeId();

    /**
     * The relocating node id the shard is either relocating to or relocating from.
     */
    String relocatingNodeId();

    /**
     * If the shard is relocating, return a shard routing representing the target shard or null o.w.
     * The target shard routing will be the INITIALIZING state and have relocatingNodeId set to the
     * source node.
     */
    ShardRouting targetRoutingIfRelocating();

    /**
     * Snapshot id and repository where this shard is being restored from
     */
    RestoreSource restoreSource();

    /**
     * Returns <code>true</code> iff this shard is a primary.
     */
    boolean primary();

    /**
     * A short description of the shard.
     */
    String shortSummary();

    /**
     * A shard iterator with just this shard in it.
     */
    ShardIterator shardsIt();

    /**
     * Does not write index name and shard id
     */
    void writeToThin(StreamOutput out) throws IOException;

    void readFromThin(StreamInput in) throws ClassNotFoundException, IOException;
}
