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
package org.elasticsearch.discovery.zen.publish;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PendingClusterStatesQueueTests extends ESTestCase {

    public void testSelectNextStateToProcess_empty() {
        PendingClusterStatesQueue queue = new PendingClusterStatesQueue(logger);
        assertThat(queue.getNextClusterStateToProcess(), nullValue());
    }

    public void testSimpleQueueSameMaster() {
        ClusterName clusterName = new ClusterName("abc");
        DiscoveryNode master = new DiscoveryNode("master", DummyTransportAddress.INSTANCE, Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().put(master).masterNodeId(master.id()).localNodeId(master.id()).build();

        int numUpdates = scaledRandomIntBetween(50, 100);
        PendingClusterStatesQueue queue = new PendingClusterStatesQueue(logger);
        List<ClusterState> states = new ArrayList<>();
        for (int i = 0; i < numUpdates; i++) {
            ClusterState state = ClusterState.builder(clusterName).version(i).nodes(nodes).build();
            queue.addPending(state);
            states.add(state);
        }

        // no state is committed yet
        assertThat(queue.getNextClusterStateToProcess(), nullValue());

        ClusterState highestComitted = null;
        for (int iter = randomInt(10); iter >= 0; iter--) {
            ClusterState state = queue.markAsCommitted(randomFrom(states).stateUUID(), new MockListener());
            if (state != null) {
                if (highestComitted == null || state.supersedes(highestComitted)) {
                    highestComitted = state;
                }
            }
        }

        assertThat(queue.getNextClusterStateToProcess(), sameInstance(highestComitted));

        queue.markAsProcessed(highestComitted);

        // now there is nothing more to process
        assertThat(queue.getNextClusterStateToProcess(), nullValue());
    }

//    public void testSelectNextStateToProcess_differentMasters() {
//        ClusterName clusterName = new ClusterName("abc");
//        DiscoveryNodes nodesA = DiscoveryNodes.builder().masterNodeId("a").build();
//        DiscoveryNodes nodesB = DiscoveryNodes.builder().masterNodeId("b").build();
//
//        PendingClusterStatesQueue queue = new PendingClusterStatesQueue(logger);
//        ClusterState thirdMostRecent = ClusterState.builder(clusterName).version(1).nodes(nodesA).build();
//        queue.addPending(thirdMostRecent);
//        MockListener thirdMostRecentListener = new MockListener();
//        queue.markAsCommitted(thirdMostRecent.stateUUID(), thirdMostRecentListener);
//        ClusterState secondMostRecent = ClusterState.builder(clusterName).version(2).nodes(nodesA).build();
//        queue.addPending(secondMostRecent);
//        ClusterState mostRecent = ClusterState.builder(clusterName).version(3).nodes(nodesA).build();
//        queue.addPending(mostRecent);
//
//        queue.addPending(ClusterState.builder(clusterName).version(4).nodes(nodesB).build());
//        queue.addPending(ClusterState.builder(clusterName).version(5).nodes(nodesA).build());
//
//
//        assertThat(ZenDiscovery.selectNextStateToProcess(queue), sameInstance(mostRecent.clusterState));
//        assertThat(thirdMostRecent.processed, is(true));
//        assertThat(secondMostRecent.processed, is(true));
//        assertThat(mostRecent.processed, is(true));
//        assertThat(queue.size(), equalTo(2));
//        assertThat(queue.get(0).processed, is(false));
//        assertThat(queue.get(1).processed, is(false));
//    }


    static class MockListener implements PendingClusterStatesQueue.StateProcessedListener {
        volatile boolean processed;
        volatile Throwable failure;

        @Override
        public void onNewClusterStateProcessed() {
            processed = true;
        }

        @Override
        public void onNewClusterStateFailed(Throwable t) {
            failure = t;
        }
    }

}
