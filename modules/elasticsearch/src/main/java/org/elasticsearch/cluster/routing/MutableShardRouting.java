/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this 
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
 * @author kimchy (Shay Banon)
 */
public class MutableShardRouting extends ImmutableShardRouting {

    public MutableShardRouting() {
    }

    public MutableShardRouting(ShardRouting copy) {
        super(copy);
    }

    public MutableShardRouting(String index, int shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
        super(index, shardId, currentNodeId, primary, state);
    }

    public MutableShardRouting(String index, int shardId, String currentNodeId,
                               String relocatingNodeId, boolean primary, ShardRoutingState state) {
        super(index, shardId, currentNodeId, relocatingNodeId, primary, state);
    }

    public void assignToNode(String nodeId) {
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

    public void relocate(String relocatingNodeId) {
        assert state == ShardRoutingState.STARTED;
        state = ShardRoutingState.RELOCATING;
        this.relocatingNodeId = relocatingNodeId;
    }

    public void cancelRelocation() {
        assert state == ShardRoutingState.RELOCATING;
        assert assignedToNode();
        assert relocatingNodeId != null;

        state = ShardRoutingState.STARTED;
        relocatingNodeId = null;
    }

    public void deassignNode() {
        assert state != ShardRoutingState.UNASSIGNED;

        state = ShardRoutingState.UNASSIGNED;
        this.currentNodeId = null;
        this.relocatingNodeId = null;
    }

    public void moveToStarted() {
        assert state == ShardRoutingState.INITIALIZING || state == ShardRoutingState.RELOCATING;
        relocatingNodeId = null;
        state = ShardRoutingState.STARTED;
    }

    public void moveToPrimary() {
        if (primary) {
            throw new IllegalShardRoutingStateException(this, "Already primary, can't move to primary");
        }
        primary = true;
    }

    public void moveToBackup() {
        if (!primary) {
            throw new IllegalShardRoutingStateException(this, "Already primary, can't move to backup");
        }
        primary = false;
    }
}

