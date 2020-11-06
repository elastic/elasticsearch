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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

public class PublicationTests extends ESTestCase {

    class MockNode {

        MockNode(Settings settings, DiscoveryNode localNode) {
            this.localNode = localNode;
            ClusterState initialState = CoordinationStateTests.clusterState(0L, 0L, localNode,
                CoordinationMetadata.VotingConfiguration.EMPTY_CONFIG, CoordinationMetadata.VotingConfiguration.EMPTY_CONFIG, 0L);
            coordinationState = new CoordinationState(localNode, new InMemoryPersistedState(0L, initialState),
                ElectionStrategy.DEFAULT_INSTANCE);
        }

        final DiscoveryNode localNode;

        final CoordinationState coordinationState;

        public MockPublication publish(ClusterState clusterState, Discovery.AckListener ackListener, Set<DiscoveryNode> faultyNodes) {
            PublishRequest publishRequest = coordinationState.handleClientValue(clusterState);
            MockPublication currentPublication = new MockPublication(publishRequest, ackListener, () -> 0L) {
                @Override
                protected boolean isPublishQuorum(CoordinationState.VoteCollection votes) {
                    return coordinationState.isPublishQuorum(votes);
                }

                @Override
                protected Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {
                    return coordinationState.handlePublishResponse(sourceNode, publishResponse);
                }
            };
            currentPublication.start(faultyNodes);
            return currentPublication;
        }
    }

    abstract class MockPublication extends Publication {

        final PublishRequest publishRequest;

        ApplyCommitRequest applyCommit;

        boolean completed;

        boolean committed;

        Map<DiscoveryNode, ActionListener<PublishWithJoinResponse>> pendingPublications = new LinkedHashMap<>();
        Map<DiscoveryNode, ActionListener<TransportResponse.Empty>> pendingCommits = new LinkedHashMap<>();
        Map<DiscoveryNode, Join> joins = new HashMap<>();
        Set<DiscoveryNode> missingJoins = new HashSet<>();

        MockPublication(PublishRequest publishRequest, Discovery.AckListener ackListener, LongSupplier currentTimeSupplier) {
            super(publishRequest, ackListener, currentTimeSupplier);
            this.publishRequest = publishRequest;
        }

        @Override
        protected void onCompletion(boolean committed) {
            assertFalse(completed);
            completed = true;
            this.committed = committed;
        }

        @Override
        protected void onJoin(Join join) {
            assertNull(joins.put(join.getSourceNode(), join));
        }

        @Override
        protected void onMissingJoin(DiscoveryNode discoveryNode) {
            assertTrue(missingJoins.add(discoveryNode));
        }

        @Override
        protected void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                          ActionListener<PublishWithJoinResponse> responseActionListener) {
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

    DiscoveryNode n1 = CoordinationStateTests.createNode("node1");
    DiscoveryNode n2 = CoordinationStateTests.createNode("node2");
    DiscoveryNode n3 = CoordinationStateTests.createNode("node3");
    Set<DiscoveryNode> discoNodes = Sets.newHashSet(n1, n2, n3);

    MockNode node1 = new MockNode(Settings.EMPTY, n1);
    MockNode node2 = new MockNode(Settings.EMPTY, n2);
    MockNode node3 = new MockNode(Settings.EMPTY, n3);
    List<MockNode> nodes = Arrays.asList(node1, node2, node3);

    Function<DiscoveryNode, MockNode> nodeResolver = dn -> nodes.stream().filter(mn -> mn.localNode.equals(dn)).findFirst().get();

    private void initializeCluster(VotingConfiguration initialConfig) {
        node1.coordinationState.setInitialState(CoordinationStateTests.clusterState(0L, 0L, n1, initialConfig, initialConfig, 0L));
        StartJoinRequest startJoinRequest = new StartJoinRequest(n1, 1L);
        node1.coordinationState.handleJoin(node1.coordinationState.handleStartJoin(startJoinRequest));
        node1.coordinationState.handleJoin(node2.coordinationState.handleStartJoin(startJoinRequest));
        node1.coordinationState.handleJoin(node3.coordinationState.handleStartJoin(startJoinRequest));
        assertTrue(node1.coordinationState.electionWon());
    }

    public void testSimpleClusterStatePublishing() throws InterruptedException {
        VotingConfiguration singleNodeConfig = VotingConfiguration.of(n1);
        initializeCluster(singleNodeConfig);

        AssertingAckListener ackListener = new AssertingAckListener(nodes.size());
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(n1).add(n2).add(n3).localNodeId(n1.getId()).build();
        MockPublication publication = node1.publish(CoordinationStateTests.clusterState(1L, 2L,
            discoveryNodes, singleNodeConfig, singleNodeConfig, 42L), ackListener, Collections.emptySet());

        assertThat(publication.pendingPublications.keySet(), equalTo(discoNodes));
        assertThat(publication.completedNodes(), empty());
        assertTrue(publication.pendingCommits.isEmpty());
        AtomicBoolean processedNode1PublishResponse = new AtomicBoolean();
        boolean delayProcessingNode2PublishResponse = randomBoolean();
        publication.pendingPublications.entrySet().stream().collect(shuffle()).forEach(e -> {
            if (delayProcessingNode2PublishResponse && e.getKey().equals(n2)) {
                return;
            }
            PublishResponse publishResponse = nodeResolver.apply(e.getKey()).coordinationState.handlePublishRequest(
                publication.publishRequest);
            assertNotEquals(processedNode1PublishResponse.get(), publication.pendingCommits.isEmpty());
            assertFalse(publication.joins.containsKey(e.getKey()));
            PublishWithJoinResponse publishWithJoinResponse = new PublishWithJoinResponse(publishResponse,
                randomBoolean() ? Optional.empty() : Optional.of(new Join(e.getKey(), randomFrom(n1, n2, n3), publishResponse.getTerm(),
                    randomNonNegativeLong(), randomNonNegativeLong())));
            e.getValue().onResponse(publishWithJoinResponse);
            if (publishWithJoinResponse.getJoin().isPresent()) {
                assertTrue(publication.joins.containsKey(e.getKey()));
                assertFalse(publication.missingJoins.contains(e.getKey()));
                assertEquals(publishWithJoinResponse.getJoin().get(), publication.joins.get(e.getKey()));
            } else {
                assertFalse(publication.joins.containsKey(e.getKey()));
                assertTrue(publication.missingJoins.contains(e.getKey()));
            }
            if (e.getKey().equals(n1)) {
                processedNode1PublishResponse.set(true);
            }
            assertNotEquals(processedNode1PublishResponse.get(), publication.pendingCommits.isEmpty());
        });

        if (delayProcessingNode2PublishResponse) {
            assertThat(publication.pendingCommits.keySet(), equalTo(Sets.newHashSet(n1, n3)));
        } else {
            assertThat(publication.pendingCommits.keySet(), equalTo(discoNodes));
        }
        assertNotNull(publication.applyCommit);
        assertEquals(publication.applyCommit.getTerm(), publication.publishRequest.getAcceptedState().term());
        assertEquals(publication.applyCommit.getVersion(), publication.publishRequest.getAcceptedState().version());
        publication.pendingCommits.entrySet().stream().collect(shuffle()).forEach(e -> {
            assertFalse(publication.completed);
            assertFalse(publication.committed);
            nodeResolver.apply(e.getKey()).coordinationState.handleCommit(publication.applyCommit);
            e.getValue().onResponse(TransportResponse.Empty.INSTANCE);
        });

        if (delayProcessingNode2PublishResponse) {
            assertFalse(publication.completed);
            assertFalse(publication.committed);
            PublishResponse publishResponse = nodeResolver.apply(n2).coordinationState.handlePublishRequest(
                publication.publishRequest);
            publication.pendingPublications.get(n2).onResponse(new PublishWithJoinResponse(publishResponse, Optional.empty()));
            assertThat(publication.pendingCommits.keySet(), equalTo(discoNodes));

            assertFalse(publication.completed);
            assertFalse(publication.committed);
            assertThat(publication.completedNodes(), containsInAnyOrder(n1, n3));
            publication.pendingCommits.get(n2).onResponse(TransportResponse.Empty.INSTANCE);
        }

        assertTrue(publication.completed);
        assertThat(publication.completedNodes(), containsInAnyOrder(n1, n2, n3));
        assertTrue(publication.committed);

        assertThat(ackListener.await(0L, TimeUnit.SECONDS), containsInAnyOrder(n1, n2, n3));
    }

    public void testClusterStatePublishingWithFaultyNodeBeforeCommit() throws InterruptedException {
        VotingConfiguration singleNodeConfig = VotingConfiguration.of(n1);
        initializeCluster(singleNodeConfig);

        AssertingAckListener ackListener = new AssertingAckListener(nodes.size());
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(n1).add(n2).add(n3).localNodeId(n1.getId()).build();

        AtomicInteger remainingActions = new AtomicInteger(4); // number of publish actions + initial faulty nodes injection
        int injectFaultAt = randomInt(remainingActions.get() - 1);
        logger.info("Injecting fault at: {}", injectFaultAt);

        Set<DiscoveryNode> initialFaultyNodes = remainingActions.decrementAndGet() == injectFaultAt ?
            Collections.singleton(n2) : Collections.emptySet();
        MockPublication publication = node1.publish(CoordinationStateTests.clusterState(1L, 2L,
            discoveryNodes, singleNodeConfig, singleNodeConfig, 42L), ackListener, initialFaultyNodes);

        publication.pendingPublications.entrySet().stream().collect(shuffle()).forEach(e -> {
            if (remainingActions.decrementAndGet() == injectFaultAt) {
                publication.onFaultyNode(n2);
            }
            if (e.getKey().equals(n2) == false || randomBoolean()) {
                PublishResponse publishResponse = nodeResolver.apply(e.getKey()).coordinationState.handlePublishRequest(
                    publication.publishRequest);
                e.getValue().onResponse(new PublishWithJoinResponse(publishResponse, Optional.empty()));
            }
        });

        publication.pendingCommits.entrySet().stream().collect(shuffle()).forEach(e -> {
            nodeResolver.apply(e.getKey()).coordinationState.handleCommit(publication.applyCommit);
            e.getValue().onResponse(TransportResponse.Empty.INSTANCE);
        });

        assertTrue(publication.completed);
        assertTrue(publication.committed);

        publication.onFaultyNode(randomFrom(n1, n3)); // has no influence

        List<Tuple<DiscoveryNode, Throwable>> errors = ackListener.awaitErrors(0L, TimeUnit.SECONDS);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0).v1(), equalTo(n2));
        assertThat(errors.get(0).v2().getMessage(), containsString("faulty node"));
    }

    public void testClusterStatePublishingWithFaultyNodeAfterCommit() throws InterruptedException {
        VotingConfiguration singleNodeConfig = VotingConfiguration.of(n1);
        initializeCluster(singleNodeConfig);

        AssertingAckListener ackListener = new AssertingAckListener(nodes.size());
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(n1).add(n2).add(n3).localNodeId(n1.getId()).build();

        boolean publicationDidNotMakeItToNode2 = randomBoolean();
        AtomicInteger remainingActions = new AtomicInteger(publicationDidNotMakeItToNode2 ? 2 : 3);
        int injectFaultAt = randomInt(remainingActions.get() - 1);
        logger.info("Injecting fault at: {}, publicationDidNotMakeItToNode2: {}", injectFaultAt, publicationDidNotMakeItToNode2);

        MockPublication publication = node1.publish(CoordinationStateTests.clusterState(1L, 2L,
            discoveryNodes, singleNodeConfig, singleNodeConfig, 42L), ackListener, Collections.emptySet());

        publication.pendingPublications.entrySet().stream().collect(shuffle()).forEach(e -> {
            if (e.getKey().equals(n2) == false || publicationDidNotMakeItToNode2 == false) {
                PublishResponse publishResponse = nodeResolver.apply(e.getKey()).coordinationState.handlePublishRequest(
                    publication.publishRequest);
                e.getValue().onResponse(new PublishWithJoinResponse(publishResponse, Optional.empty()));
            }
        });

        publication.pendingCommits.entrySet().stream().collect(shuffle()).forEach(e -> {
            if (e.getKey().equals(n2)) {
                // we must fail node before committing for the node, otherwise failing the node is ignored
                publication.onFaultyNode(n2);
            }
            if (remainingActions.decrementAndGet() == injectFaultAt) {
                publication.onFaultyNode(n2);
            }
            if (e.getKey().equals(n2) == false || randomBoolean()) {
                nodeResolver.apply(e.getKey()).coordinationState.handleCommit(publication.applyCommit);
                e.getValue().onResponse(TransportResponse.Empty.INSTANCE);
            }
        });

        // we need to complete publication by failing the node
        if (publicationDidNotMakeItToNode2 && remainingActions.get() > injectFaultAt) {
            publication.onFaultyNode(n2);
        }

        assertTrue(publication.completed);
        assertTrue(publication.committed);

        publication.onFaultyNode(randomFrom(n1, n3)); // has no influence

        List<Tuple<DiscoveryNode, Throwable>> errors = ackListener.awaitErrors(0L, TimeUnit.SECONDS);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0).v1(), equalTo(n2));
        assertThat(errors.get(0).v2().getMessage(), containsString("faulty node"));
    }

    public void testClusterStatePublishingFailsOrTimesOutBeforeCommit() throws InterruptedException {
        VotingConfiguration config = VotingConfiguration.of(n1, n2);
        initializeCluster(config);

        AssertingAckListener ackListener = new AssertingAckListener(nodes.size());
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(n1).add(n2).add(n3).localNodeId(n1.getId()).build();
        MockPublication publication = node1.publish(CoordinationStateTests.clusterState(1L, 2L,
            discoveryNodes, config, config, 42L), ackListener, Collections.emptySet());

        boolean timeOut = randomBoolean();
        publication.pendingPublications.entrySet().stream().collect(shuffle()).forEach(e -> {
            if (e.getKey().equals(n2)) {
                if (timeOut) {
                    publication.cancel("timed out");
                } else {
                    e.getValue().onFailure(new TransportException(new Exception("dummy failure")));
                }
                assertTrue(publication.completed);
                assertFalse(publication.committed);
            } else if (randomBoolean()) {
                PublishResponse publishResponse = nodeResolver.apply(e.getKey()).coordinationState.handlePublishRequest(
                    publication.publishRequest);
                e.getValue().onResponse(new PublishWithJoinResponse(publishResponse, Optional.empty()));
            }
        });

        assertThat(publication.pendingCommits.keySet(), equalTo(Collections.emptySet()));
        assertNull(publication.applyCommit);
        assertTrue(publication.completed);
        assertFalse(publication.committed);

        List<Tuple<DiscoveryNode, Throwable>> errors = ackListener.awaitErrors(0L, TimeUnit.SECONDS);
        assertThat(errors.size(), equalTo(3));
        assertThat(errors.stream().map(Tuple::v1).collect(Collectors.toList()), containsInAnyOrder(n1, n2, n3));
        errors.stream().forEach(tuple ->
            assertThat(tuple.v2().getMessage(), containsString(timeOut ? "timed out" :
                tuple.v1().equals(n2) ? "dummy failure" : "non-failed nodes do not form a quorum")));
    }

    public void testPublishingToMastersFirst() {
        VotingConfiguration singleNodeConfig = VotingConfiguration.of(n1);
        initializeCluster(singleNodeConfig);

        DiscoveryNodes.Builder discoNodesBuilder = DiscoveryNodes.builder();
        randomNodes(10).forEach(dn -> discoNodesBuilder.add(dn));
        DiscoveryNodes discoveryNodes = discoNodesBuilder.add(n1).localNodeId(n1.getId()).build();
        MockPublication publication = node1.publish(CoordinationStateTests.clusterState(1L, 2L,
            discoveryNodes, singleNodeConfig, singleNodeConfig, 42L), null, Collections.emptySet());

        List<DiscoveryNode> publicationTargets = new ArrayList<>(publication.pendingPublications.keySet());
        List<DiscoveryNode> sortedPublicationTargets = new ArrayList<>(publicationTargets);
        Collections.sort(sortedPublicationTargets, Comparator.comparing(n -> n.isMasterNode() == false));
        assertEquals(sortedPublicationTargets, publicationTargets);
    }

    public void testClusterStatePublishingTimesOutAfterCommit() throws InterruptedException {
        VotingConfiguration config = randomBoolean() ? VotingConfiguration.of(n1, n2): VotingConfiguration.of(n1, n2, n3);
        initializeCluster(config);

        AssertingAckListener ackListener = new AssertingAckListener(nodes.size());
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(n1).add(n2).add(n3).localNodeId(n1.getId()).build();
        MockPublication publication = node1.publish(CoordinationStateTests.clusterState(1L, 2L,
            discoveryNodes, config, config, 42L), ackListener, Collections.emptySet());

        boolean publishedToN3 = randomBoolean();
        publication.pendingPublications.entrySet().stream().collect(shuffle()).forEach(e -> {
            if (e.getKey().equals(n3) == false || publishedToN3) {
                PublishResponse publishResponse = nodeResolver.apply(e.getKey()).coordinationState.handlePublishRequest(
                    publication.publishRequest);
                e.getValue().onResponse(new PublishWithJoinResponse(publishResponse, Optional.empty()));
            }
        });

        assertNotNull(publication.applyCommit);

        Set<DiscoveryNode> committingNodes = new HashSet<>(randomSubsetOf(discoNodes));
        if (publishedToN3 == false) {
            committingNodes.remove(n3);
        }

        logger.info("Committing nodes: {}", committingNodes);

        publication.pendingCommits.entrySet().stream().collect(shuffle()).forEach(e -> {
            if (committingNodes.contains(e.getKey())) {
                nodeResolver.apply(e.getKey()).coordinationState.handleCommit(publication.applyCommit);
                e.getValue().onResponse(TransportResponse.Empty.INSTANCE);
            }
        });

        publication.cancel("timed out");
        assertTrue(publication.completed);
        assertTrue(publication.committed);
        assertEquals(committingNodes, ackListener.await(0L, TimeUnit.SECONDS));

        // check that acking still works after publication completed
        if (publishedToN3 == false) {
            publication.pendingPublications.get(n3).onResponse(
                new PublishWithJoinResponse(node3.coordinationState.handlePublishRequest(publication.publishRequest), Optional.empty()));
        }

        assertEquals(discoNodes, publication.pendingCommits.keySet());

        Set<DiscoveryNode> nonCommittedNodes = Sets.difference(discoNodes, committingNodes);
        logger.info("Non-committed nodes: {}", nonCommittedNodes);
        nonCommittedNodes.stream().collect(shuffle()).forEach(n ->
            publication.pendingCommits.get(n).onResponse(TransportResponse.Empty.INSTANCE));

        assertEquals(discoNodes, ackListener.await(0L, TimeUnit.SECONDS));
    }

    private static List<DiscoveryNode> randomNodes(final int numNodes) {
        List<DiscoveryNode> nodesList = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            if (frequently()) {
                attributes.put("custom", randomBoolean() ? "match" : randomAlphaOfLengthBetween(3, 5));
            }
            final DiscoveryNode node = newNode(i, attributes,
                new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES)));
            nodesList.add(node);
        }
        return nodesList;
    }

    private static DiscoveryNode newNode(int nodeId, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode("name_" + nodeId, "node_" + nodeId, buildNewFakeTransportAddress(), attributes, roles,
            Version.CURRENT);
    }

    public static <T> Collector<T, ?, Stream<T>> shuffle() {
        return Collectors.collectingAndThen(Collectors.toList(),
            ts -> {
                Collections.shuffle(ts, random());
                return ts.stream();
            });
    }

    public static class AssertingAckListener implements Discovery.AckListener {
        private final List<Tuple<DiscoveryNode, Throwable>> errors = new CopyOnWriteArrayList<>();
        private final Set<DiscoveryNode> successfulAcks = Collections.synchronizedSet(new HashSet<>());
        private final CountDownLatch countDown;
        private final CountDownLatch commitCountDown;

        public AssertingAckListener(int nodeCount) {
            countDown = new CountDownLatch(nodeCount);
            commitCountDown = new CountDownLatch(1);
        }

        @Override
        public void onCommit(TimeValue commitTime) {
            commitCountDown.countDown();
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            if (e != null) {
                errors.add(new Tuple<>(node, e));
            } else {
                successfulAcks.add(node);
            }
            countDown.countDown();
        }

        public Set<DiscoveryNode> await(long timeout, TimeUnit unit) throws InterruptedException {
            assertThat(awaitErrors(timeout, unit), emptyIterable());
            assertTrue(commitCountDown.await(timeout, unit));
            return new HashSet<>(successfulAcks);
        }

        public List<Tuple<DiscoveryNode, Throwable>> awaitErrors(long timeout, TimeUnit unit) throws InterruptedException {
            countDown.await(timeout, unit);
            return errors;
        }

    }
}
