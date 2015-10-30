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

import org.elasticsearch.test.ESTestCase;

/**
 * A helper that allows to create shard routing instances within tests, while not requiring to expose
 * different simplified constructors on the ShardRouting itself.
 */
public class TestShardRouting {

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, boolean primary, ShardRoutingState state, long version) {
        return new ShardRouting(index, shardId, currentNodeId, null, null, primary, state, version, buildUnassignedInfo(state), buildAllocationId(state), true, -1);
    }

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, String relocatingNodeId, boolean primary, ShardRoutingState state, long version) {
        return new ShardRouting(index, shardId, currentNodeId, relocatingNodeId, null, primary, state, version, buildUnassignedInfo(state), buildAllocationId(state), true, -1);
    }

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, String relocatingNodeId, boolean primary, ShardRoutingState state, AllocationId allocationId, long version) {
        return new ShardRouting(index, shardId, currentNodeId, relocatingNodeId, null, primary, state, version, buildUnassignedInfo(state), allocationId, true, -1);
    }

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, String relocatingNodeId, RestoreSource restoreSource, boolean primary, ShardRoutingState state, long version) {
        return new ShardRouting(index, shardId, currentNodeId, relocatingNodeId, restoreSource, primary, state, version, buildUnassignedInfo(state), buildAllocationId(state), true, -1);
    }

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId,
                                               String relocatingNodeId, RestoreSource restoreSource, boolean primary, ShardRoutingState state, long version,
                                               UnassignedInfo unassignedInfo) {
        return new ShardRouting(index, shardId, currentNodeId, relocatingNodeId, restoreSource, primary, state, version, unassignedInfo, buildAllocationId(state), true, -1);
    }

    private static AllocationId buildAllocationId(ShardRoutingState state) {
        switch (state) {
            case UNASSIGNED:
                return null;
            case INITIALIZING:
            case STARTED:
                return AllocationId.newInitializing();
            case RELOCATING:
                AllocationId allocationId = AllocationId.newInitializing();
                return AllocationId.newRelocation(allocationId);
            default:
                throw new IllegalStateException("illegal state");
        }
    }

    private static UnassignedInfo buildUnassignedInfo(ShardRoutingState state) {
        switch (state) {
            case UNASSIGNED:
            case INITIALIZING:
                return new UnassignedInfo(ESTestCase.randomFrom(UnassignedInfo.Reason.values()), "auto generated for test");
            case STARTED:
            case RELOCATING:
                return null;
            default:
                throw new IllegalStateException("illegal state");
        }
    }
}
