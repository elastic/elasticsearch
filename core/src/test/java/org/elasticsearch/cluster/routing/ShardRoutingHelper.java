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
 * A helper class that allows access to package private APIs for testing.
 */
public class ShardRoutingHelper {

    public static ShardRouting relocate(ShardRouting routing, String nodeId) {
        return relocate(routing, nodeId, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    public static ShardRouting relocate(ShardRouting routing, String nodeId, long expectedByteSize) {
        return routing.relocate(nodeId, expectedByteSize);
    }

    public static ShardRouting moveToStarted(ShardRouting routing) {
        return routing.moveToStarted();
    }

    public static ShardRouting initialize(ShardRouting routing, String nodeId) {
        return initialize(routing, nodeId, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    public static ShardRouting initialize(ShardRouting routing, String nodeId, long expectedSize) {
        return routing.initialize(nodeId, null, expectedSize);
    }

    public static ShardRouting reinit(ShardRouting routing) {
        return routing.reinitializeShard();
    }

    public static ShardRouting reinit(ShardRouting routing, UnassignedInfo.Reason reason) {
        return routing.reinitializeShard().updateUnassignedInfo(new UnassignedInfo(reason, "test_reinit"));
    }

    public static ShardRouting initWithSameId(ShardRouting copy) {
        return new ShardRouting(copy.shardId(), copy.currentNodeId(), copy.relocatingNodeId(), copy.restoreSource(),
            copy.primary(), ShardRoutingState.INITIALIZING, new UnassignedInfo(UnassignedInfo.Reason.REINITIALIZED, null),
            copy.allocationId(), copy.getExpectedShardSize());
    }

    public static ShardRouting moveToUnassigned(ShardRouting routing, UnassignedInfo info) {
        return routing.moveToUnassigned(info);
    }

    public static ShardRouting newWithRestoreSource(ShardRouting routing, RestoreSource restoreSource) {
        return new ShardRouting(routing.shardId(), routing.currentNodeId(), routing.relocatingNodeId(), restoreSource,
            routing.primary(), routing.state(), routing.unassignedInfo(), routing.allocationId(), routing.getExpectedShardSize());
    }
}
