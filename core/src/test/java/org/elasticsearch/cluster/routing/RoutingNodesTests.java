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

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RoutingNodesTests extends ESTestCase {
    public void testMarkShardAsResyncFailed() throws Exception {
        ClusterState clusterState = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(
            generateRandomStringArray(10, 100, false, false), between(5, 10), between(1, 3));
        final ShardRouting shardToMark = randomFrom(clusterState.routingTable().allShards());
        RoutingNodes routingNodes = new RoutingNodes(clusterState, false);
        final List<ShardRouting> shardsBeforeMarking = routingNodes.shards(shardRouting -> true);
        ResyncFailedInfo resyncFailedInfo = new ResyncFailedInfo(randomAlphaOfLengthBetween(10, 100),
            new IOException(randomAlphaOfLengthBetween(10, 100)));
        final ShardRouting markedShard = routingNodes.markShardResyncFailed(shardToMark, resyncFailedInfo);
        assertThat(markedShard.equalsIgnoringMetaData(shardToMark), equalTo(true));
        assertThat(markedShard.resyncFailedInfo(), equalTo(resyncFailedInfo));
        final List<ShardRouting> shardsAfterMarking = routingNodes.shards(shardRouting -> true);
        final Set<ShardRouting> markedShards = Sets.difference(new HashSet<>(shardsAfterMarking), new HashSet<>(shardsBeforeMarking));
        assertThat(markedShards, hasSize(1));
        assertThat(markedShards, contains(markedShard));
    }
}
