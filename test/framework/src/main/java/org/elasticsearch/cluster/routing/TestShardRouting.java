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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;

/**
 * A helper that allows to create shard routing instances within tests, while not requiring to expose
 * different simplified constructors on the ShardRouting itself.
 */
public class TestShardRouting {

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
        return newShardRouting(new ShardId(index, IndexMetaData.INDEX_UUID_NA_VALUE, shardId), currentNodeId, primary, state);
    }

    public static ShardRouting newShardRouting(ShardId shardId, String currentNodeId, boolean primary, RecoverySource recoverySource, ShardRoutingState state) {
        return new ShardRouting(shardId, currentNodeId, null, primary, state, recoverySource, buildUnassignedInfo(state), buildAllocationId(state), -1);
    }

    public static ShardRouting newShardRouting(ShardId shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
        return new ShardRouting(shardId, currentNodeId, null, primary, state, buildRecoveryTarget(primary, state), buildUnassignedInfo(state), buildAllocationId(state), -1);
    }

    public static ShardRouting newShardRouting(ShardId shardId, String currentNodeId, boolean primary, ShardRoutingState state, RecoverySource recoverySource) {
        return new ShardRouting(shardId, currentNodeId, null, primary, state, recoverySource, buildUnassignedInfo(state), buildAllocationId(state), -1);
    }

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, String relocatingNodeId, boolean primary, ShardRoutingState state) {
        return newShardRouting(new ShardId(index, IndexMetaData.INDEX_UUID_NA_VALUE, shardId), currentNodeId, relocatingNodeId, primary, state);
    }

    public static ShardRouting newShardRouting(ShardId shardId, String currentNodeId, String relocatingNodeId, boolean primary, ShardRoutingState state) {
        return new ShardRouting(shardId, currentNodeId, relocatingNodeId, primary, state, buildRecoveryTarget(primary, state), buildUnassignedInfo(state), buildAllocationId(state), -1);
    }

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, String relocatingNodeId, boolean primary, ShardRoutingState state, AllocationId allocationId) {
        return newShardRouting(new ShardId(index, IndexMetaData.INDEX_UUID_NA_VALUE, shardId), currentNodeId, relocatingNodeId, primary, state, allocationId);
    }

    public static ShardRouting newShardRouting(ShardId shardId, String currentNodeId, String relocatingNodeId, boolean primary, ShardRoutingState state, AllocationId allocationId) {
        return new ShardRouting(shardId, currentNodeId, relocatingNodeId, primary, state, buildRecoveryTarget(primary, state), buildUnassignedInfo(state), allocationId, -1);
    }

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId,
                                               String relocatingNodeId, boolean primary, ShardRoutingState state,
                                               UnassignedInfo unassignedInfo) {
        return newShardRouting(new ShardId(index, IndexMetaData.INDEX_UUID_NA_VALUE, shardId), currentNodeId, relocatingNodeId, primary, state, unassignedInfo);
    }

    public static ShardRouting newShardRouting(ShardId shardId, String currentNodeId,
                                               String relocatingNodeId, boolean primary, ShardRoutingState state,
                                               UnassignedInfo unassignedInfo) {
        return new ShardRouting(shardId, currentNodeId, relocatingNodeId, primary, state, buildRecoveryTarget(primary, state), unassignedInfo, buildAllocationId(state), -1);
    }

    public static ShardRouting relocate(ShardRouting shardRouting, String relocatingNodeId, long expectedShardSize) {
        return shardRouting.relocate(relocatingNodeId, expectedShardSize);
    }

    private static RecoverySource buildRecoveryTarget(boolean primary, ShardRoutingState state) {
        switch (state) {
            case UNASSIGNED:
            case INITIALIZING:
                if (primary) {
                    return ESTestCase.randomFrom(RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE,
                        RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE);
                } else {
                    return RecoverySource.PeerRecoverySource.INSTANCE;
                }
            case STARTED:
            case RELOCATING:
                return null;
            default:
                throw new IllegalStateException("illegal state");
        }
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

    public static RecoverySource randomRecoverySource() {
        return ESTestCase.randomFrom(RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE,
            RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE,
            RecoverySource.PeerRecoverySource.INSTANCE,
            RecoverySource.LocalShardsRecoverySource.INSTANCE,
            new RecoverySource.SnapshotRecoverySource(
                new Snapshot("repo", new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID())),
                Version.CURRENT,
                "some_index"));
    }
}
