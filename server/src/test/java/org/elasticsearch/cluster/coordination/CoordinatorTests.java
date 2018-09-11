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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.coordination.CoordinationStateTests.value;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class CoordinatorTests extends ESTestCase {

    public void testCanProposeValueAfterStabilisation() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly();
        cluster.stabilise();
        final Cluster.ClusterNode leader = cluster.getAnyLeader();
        final long stabilisedVersion = leader.coordinator.getLastCommittedState().get().getVersion();

        final long finalValue = randomLong();
        logger.info("--> proposing final value [{}] to [{}]", finalValue, leader.getId());
        leader.submitValue(finalValue);
        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);

        for (final Cluster.ClusterNode clusterNode : cluster.clusterNodes) {
            final String legislatorId = clusterNode.getId();
            final ClusterState committedState = clusterNode.coordinator.getLastCommittedState().get();
            assertThat(legislatorId + " is at the next version", committedState.getVersion(), equalTo(stabilisedVersion + 1));
            assertThat(legislatorId + " has the right value", value(committedState), is(finalValue));
        }
    }

    class Cluster {

        static final long DEFAULT_DELAY_VARIABILITY = 100L;

        final List<ClusterNode> clusterNodes;

        Cluster(int initialNodeCount) {
            logger.info("--> creating cluster of {} nodes", initialNodeCount);
            clusterNodes = new ArrayList<>(initialNodeCount);
            for (int i = 0; i < initialNodeCount; i++) {
                final ClusterNode clusterNode = new ClusterNode(i);
                clusterNodes.add(clusterNode);
            }

            for (final ClusterNode clusterNode : clusterNodes) {
                clusterNode.initialise();
            }
        }

        void runRandomly() {

        }

        void stabilise() {

        }

        void stabilise(long delayVariability, long stabilisationTime) {

        }

        ClusterNode getAnyLeader() {
            List<ClusterNode> allLeaders = clusterNodes.stream().filter(ClusterNode::isLeader).collect(Collectors.toList());
            assertThat(allLeaders, not(empty()));
            return randomFrom(allLeaders);
        }

        class ClusterNode {

            Coordinator coordinator;
            private DiscoveryNode localNode;

            ClusterNode(int nodeIndex) {

            }

            String getId() {
                return localNode.getId();
            }

            void submitValue(long value) {

            }

            public DiscoveryNode getLocalNode() {
                return localNode;
            }

            boolean isLeader() {
                return coordinator.getMode() == Coordinator.Mode.LEADER;
            }

            void initialise() {
            }
        }
    }
}
