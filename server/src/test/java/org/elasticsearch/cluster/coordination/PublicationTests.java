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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportResponse;

import java.util.Collections;
import java.util.Optional;
import java.util.function.LongSupplier;

public class PublicationTests extends ESTestCase {

    class MockNode {

        MockNode(Settings settings, DiscoveryNode localNode) {
            ClusterState initialState = CoordinationStateTests.clusterState(0L, 0L, localNode, ClusterState.VotingConfiguration.EMPTY_CONFIG, ClusterState.VotingConfiguration.EMPTY_CONFIG, 42L);

            coordinationState = new CoordinationState(settings, localNode, new CoordinationStateTests.InMemoryPersistedState(0L,
                ))ClusterState.builder(CLUSTER_NAME).nodes(DiscoveryNodes.builder()
                .add(discoveryNode).localNodeId(discoveryNode.getId()).build()).build();
        }

        final CoordinationState coordinationState;

        Publication currentPublication;

        public void publish(ClusterState clusterState) {
            PublishRequest publishRequest = coordinationState.handleClientValue(clusterState);
            currentPublication = new MockPublication(this, Settings.EMPTY, publishRequest, new Discovery.AckListener() {

                @Override
                public void onCommit(TimeValue commitTime) {

                }

                @Override
                public void onNodeAck(DiscoveryNode node, Exception e) {

                }
            }, () -> 0L);
            currentPublication.start(Collections.emptySet());
        }
    }

    class MockPublication extends Publication {

        private final MockNode mockNode;

        public MockPublication(MockNode mockNode, Settings settings, PublishRequest publishRequest, Discovery.AckListener ackListener,
                               LongSupplier currentTimeSupplier) {
            super(settings, publishRequest, ackListener, currentTimeSupplier);
            this.mockNode = mockNode;
        }

        @Override
        protected void onCompletion(boolean success) {
            mockNode.currentPublication = null;
        }

        @Override
        protected boolean isPublishQuorum(CoordinationState.VoteCollection votes) {
            return mockNode.coordinationState.isPublishQuorum(votes);
        }

        @Override
        protected Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {
            return mockNode.coordinationState.handlePublishResponse(sourceNode, publishResponse);
        }

        @Override
        protected void onPossibleJoin(DiscoveryNode sourceNode, LegislatorPublishResponse response) {

        }

        @Override
        protected void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                          ActionListener<LegislatorPublishResponse> responseActionListener) {

        }

        @Override
        protected void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommit,
                                       ActionListener<TransportResponse.Empty> responseActionListener) {

        }
    }

    public void testSimpleClusterStatePublishing() throws Exception {

    }


}
