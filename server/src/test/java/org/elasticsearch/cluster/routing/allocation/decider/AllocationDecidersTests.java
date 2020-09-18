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

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.List;

public class AllocationDecidersTests extends ESTestCase {

    public void testDebugMode() {
        verifyDebugMode(RoutingAllocation.DebugMode.ON, Matchers.hasSize(1));
    }

    public void testNoDebugMode() {
        verifyDebugMode(RoutingAllocation.DebugMode.OFF, Matchers.empty());
    }

    public void testDebugExcludeYesMode() {
        verifyDebugMode(RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS, Matchers.empty());
    }

    private void verifyDebugMode(RoutingAllocation.DebugMode mode, Matcher<Collection<? extends Decision>> matcher) {
        AllocationDeciders deciders = new AllocationDeciders(List.of(new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canRebalance(RoutingAllocation allocation) {
                return Decision.YES;
            }
        }));

        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();
        final RoutingAllocation allocation = new RoutingAllocation(deciders,
            clusterState.getRoutingNodes(), clusterState, null, 0L);

        allocation.setDebugMode(mode);
        final UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "_message");
        final ShardRouting shardRouting = ShardRouting.newUnassigned(new ShardId("test", "testUUID", 0), true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE, unassignedInfo);
        IndexMetadata idx =
            IndexMetadata.builder("idx").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).build();

        RoutingNode routingNode = new RoutingNode("testNode", null);
        verify(deciders.canAllocate(shardRouting, routingNode, allocation), matcher);
        verify(deciders.canAllocate(idx, routingNode, allocation), matcher);
        verify(deciders.canAllocate(shardRouting, allocation), matcher);
        verify(deciders.canRebalance(shardRouting, allocation), matcher);
        verify(deciders.canRebalance(allocation), matcher);
        verify(deciders.canRemain(shardRouting, routingNode, allocation), matcher);
        verify(deciders.canForceAllocatePrimary(shardRouting, routingNode, allocation), matcher);
        verify(deciders.shouldAutoExpandToNode(idx, null, allocation), matcher);
    }

    private void verify(Decision decision, Matcher<Collection<? extends Decision>> matcher) {
        assertThat(decision.type(), Matchers.equalTo(Decision.Type.YES));
        assertThat(decision, Matchers.instanceOf(Decision.Multi.class));
        Decision.Multi multi = (Decision.Multi) decision;
        assertThat(multi.getDecisions(), matcher);
    }
}
