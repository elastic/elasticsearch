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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.PublishClusterStateActionTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportResponse;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;

public class PublicationTests extends ESTestCase {

    class MockNode {

        MockNode(Settings settings, DiscoveryNode localNode) {
            this.localNode = localNode;
            ClusterState initialState = CoordinationStateTests.clusterState(0L, 0L, localNode,
                ClusterState.VotingConfiguration.EMPTY_CONFIG, ClusterState.VotingConfiguration.EMPTY_CONFIG, 0L);
            coordinationState = new CoordinationState(settings, localNode, new CoordinationStateTests.InMemoryPersistedState(0L,
                initialState));
        }

        final DiscoveryNode localNode;

        final CoordinationState coordinationState;

        public MockPublication publish(ClusterState clusterState, Discovery.AckListener ackListener) {
            PublishRequest publishRequest = coordinationState.handleClientValue(clusterState);
            MockPublication currentPublication = new MockPublication(Settings.EMPTY, publishRequest, ackListener, () -> 0L) {
                @Override
                protected boolean isPublishQuorum(CoordinationState.VoteCollection votes) {
                    return coordinationState.isPublishQuorum(votes);
                }

                @Override
                protected Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {
                    return coordinationState.handlePublishResponse(sourceNode, publishResponse);
                }
            };
            currentPublication.start(Collections.emptySet());
            return currentPublication;
        }
    }

    abstract class MockPublication extends Publication {

        final PublishRequest publishRequest;

        ApplyCommitRequest applyCommit;

        boolean completed;

        boolean success;

        Map<DiscoveryNode, ActionListener<LegislatorPublishResponse>> pendingPublications = new HashMap<>();
        Map<DiscoveryNode, ActionListener<TransportResponse.Empty>> pendingCommits = new HashMap<>();

        public MockPublication(Settings settings, PublishRequest publishRequest, Discovery.AckListener ackListener,
                               LongSupplier currentTimeSupplier) {
            super(settings, publishRequest, ackListener, currentTimeSupplier);
            this.publishRequest = publishRequest;
        }

        @Override
        protected void onCompletion(boolean success) {
            completed = true;
            this.success = success;
        }

        @Override
        protected void onPossibleJoin(DiscoveryNode sourceNode, LegislatorPublishResponse response) {

        }

        @Override
        protected void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                          ActionListener<LegislatorPublishResponse> responseActionListener) {
            assertSame(publishRequest, this.publishRequest);
            assertNull(pendingPublications.put(destination, responseActionListener));
        }

        @Override
        protected void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommit,
                                       ActionListener<TransportResponse.Empty> responseActionListener) {
            if (this.applyCommit == null) {
                this.applyCommit = applyCommit;
            } else {
                assertSame(applyCommit, this.applyCommit);
            }
            assertNull(pendingCommits.put(destination, responseActionListener));
        }
    }

    public void testSimpleClusterStatePublishing() throws InterruptedException {
        DiscoveryNode n1 = CoordinationStateTests.createNode("node1");
        DiscoveryNode n2 = CoordinationStateTests.createNode("node2");
        DiscoveryNode n3 = CoordinationStateTests.createNode("node3");

        MockNode node1 = new MockNode(Settings.EMPTY, n1);
        MockNode node2 = new MockNode(Settings.EMPTY, n2);
        MockNode node3 = new MockNode(Settings.EMPTY, n3);
        List<MockNode> nodes = Arrays.asList(node1, node2, node3);

        Function<DiscoveryNode, MockNode> nodeResolver = dn -> nodes.stream().filter(mn -> mn.localNode.equals(dn)).findFirst().get();

        ClusterState.VotingConfiguration singleNodeConfig = new ClusterState.VotingConfiguration(Sets.newHashSet(node1.localNode.getId()));
        node1.coordinationState.setInitialState(
            CoordinationStateTests.clusterState(0L, 1L, n1, singleNodeConfig, singleNodeConfig, 0L));
        StartJoinRequest startJoinRequest = new StartJoinRequest(n1, 1L);
        node1.coordinationState.handleJoin(node1.coordinationState.handleStartJoin(startJoinRequest));
        assertTrue(node1.coordinationState.electionWon());
        node1.coordinationState.handleJoin(node2.coordinationState.handleStartJoin(startJoinRequest));
        node1.coordinationState.handleJoin(node3.coordinationState.handleStartJoin(startJoinRequest));

        PublishClusterStateActionTests.AssertingAckListener ackListener =
            new PublishClusterStateActionTests.AssertingAckListener(nodes.size());
        MockPublication publication = node1.publish(CoordinationStateTests.clusterState(1L, 2L,
            DiscoveryNodes.builder().add(n1).add(n2).add(n3).localNodeId(n1.getId()).build(),
            singleNodeConfig, singleNodeConfig, 42L), ackListener);

        assertThat(publication.pendingPublications.keySet(), equalTo(Sets.newHashSet(n1, n2, n3)));
        assertTrue(publication.pendingCommits.isEmpty());
        publication.pendingPublications.entrySet().stream().forEach(e -> {
            PublishResponse publishResponse = nodeResolver.apply(e.getKey()).coordinationState.handlePublishRequest(
                publication.publishRequest);
            e.getValue().onResponse(new LegislatorPublishResponse(publishResponse, Optional.empty()));
        });

        assertThat(publication.pendingCommits.keySet(), equalTo(Sets.newHashSet(n1, n2, n3)));
        publication.pendingCommits.entrySet().stream().forEach(e -> {
            nodeResolver.apply(e.getKey()).coordinationState.handleCommit(publication.applyCommit);
            e.getValue().onResponse(TransportResponse.Empty.INSTANCE);
        });

        assertTrue(publication.completed);
        assertTrue(publication.success);

        ackListener.await(0L, TimeUnit.SECONDS);
    }


}
