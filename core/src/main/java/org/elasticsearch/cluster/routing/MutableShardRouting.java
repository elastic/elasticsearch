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

/**
 * Similar to {@link ImmutableShardRouting} this class keeps metadata of the current shard. But unlike
 * {@link ImmutableShardRouting} the information kept in this class can be modified.
 * These modifications include changing the primary state, relocating and assigning the shard
 * represented by this class
 */
public class MutableShardRouting extends ImmutableShardRouting {

    public MutableShardRouting(ShardRouting copy) {
        super(copy);
    }

    public MutableShardRouting(ShardRouting copy, long version) {
        super(copy);
        this.version = version;
    }

    public MutableShardRouting(String index, int shardId, String currentNodeId, boolean primary, ShardRoutingState state, long version) {
        super(index, shardId, currentNodeId, primary, state, version);
    }

    public MutableShardRouting(String index, int shardId, String currentNodeId,
                               String relocatingNodeId, boolean primary, ShardRoutingState state, long version) {
        super(index, shardId, currentNodeId, relocatingNodeId, null, primary, state, version);
    }

    public MutableShardRouting(String index, int shardId, String currentNodeId,
                               String relocatingNodeId, RestoreSource restoreSource, boolean primary, ShardRoutingState state, long version) {
        super(index, shardId, currentNodeId, relocatingNodeId, restoreSource, primary, state, version);
    }



    /**
     * Assign this shard to a node.
     *
     * @param nodeId id of the node to assign this shard to
     */
    void assignToNode(String nodeId) {
        version++;

        if (currentNodeId == null) {
            assert state == ShardRoutingState.UNASSIGNED;

            state = ShardRoutingState.INITIALIZING;
            currentNodeId = nodeId;
            relocatingNodeId = null;
        } else if (state == ShardRoutingState.STARTED) {
            state = ShardRoutingState.RELOCATING;
            relocatingNodeId = nodeId;
        } else if (state == ShardRoutingState.RELOCATING) {
            assert nodeId.equals(relocatingNodeId);
        }
    }

    /**
     * Relocate the shard to another node.
     *
     * @param relocatingNodeId id of the node to relocate the shard
     */
    void relocate(String relocatingNodeId) {
        version++;
        assert state == ShardRoutingState.STARTED;
        state = ShardRoutingState.RELOCATING;
        this.relocatingNodeId = relocatingNodeId;
    }

    /**
     * Cancel relocation of a shard. The shards state must be set
     * to <code>RELOCATING</code>.
     */
    void cancelRelocation() {
        version++;
        assert state == ShardRoutingState.RELOCATING;
        assert assignedToNode();
        assert relocatingNodeId != null;

        state = ShardRoutingState.STARTED;
        relocatingNodeId = null;
    }

    /**
     * Moves the shard from started to initializing and bumps the version
     */
    void reinitializeShard() {
        assert state == ShardRoutingState.STARTED;
        version++;
        state = ShardRoutingState.INITIALIZING;
    }

    /**
     * Set the shards state to <code>STARTED</code>. The shards state must be
     * <code>INITIALIZING</code> or <code>RELOCATING</code>. Any relocation will be
     * canceled.
     */
    void moveToStarted() {
        version++;
        assert state == ShardRoutingState.INITIALIZING || state == ShardRoutingState.RELOCATING;
        relocatingNodeId = null;
        restoreSource = null;
        state = ShardRoutingState.STARTED;
    }

    /**
     * Make the shard primary unless it's not Primary
     * //TODO: doc exception
     */
    void moveToPrimary() {
        version++;
        if (primary) {
            throw new IllegalShardRoutingStateException(this, "Already primary, can't move to primary");
        }
        primary = true;
    }

    /**
     * Set the primary shard to non-primary
     */
    void moveFromPrimary() {
        version++;
        if (!primary) {
            throw new IllegalShardRoutingStateException(this, "Not primary, can't move to replica");
        }
        primary = false;
    }

    private long hashVersion = version-1;
    private int hashCode = 0;

    @Override
    public int hashCode() {
        hashCode = (hashVersion != version ? super.hashCode() : hashCode);
        hashVersion = version;
        return hashCode;
    }
}

