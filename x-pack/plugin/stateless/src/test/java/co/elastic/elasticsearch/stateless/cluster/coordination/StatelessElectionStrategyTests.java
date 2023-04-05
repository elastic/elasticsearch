/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.CoordinationStateRejectedException;
import org.elasticsearch.cluster.coordination.Join;
import org.elasticsearch.cluster.coordination.StartJoinRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class StatelessElectionStrategyTests extends ESTestCase {
    public void testTermIsClaimedOnNewElections() throws Exception {
        try (var fakeStatelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry())) {
            var electionStrategy = new StatelessElectionStrategy(fakeStatelessNode.objectStoreService.getTermLeaseBlobContainer());

            var localNode = newDiscoveryNode("local-node");

            for (long newTerm = 2; newTerm < 10; newTerm++) {
                long proposedTerm = newTerm;
                StartJoinRequest startJoinRequest = PlainActionFuture.get(f -> electionStrategy.onNewElection(localNode, proposedTerm, f));
                assertThat(startJoinRequest.getTerm(), is(equalTo(proposedTerm)));
                assertThat(startJoinRequest.getSourceNode(), is(equalTo(localNode)));

                var currentTerm = PlainActionFuture.get(electionStrategy::getCurrentLeaseTerm).orElseThrow();
                assertThat(currentTerm, is(equalTo(proposedTerm)));
            }

            // We read the latest term before claiming a new term
            StartJoinRequest startJoinRequest = PlainActionFuture.get(f -> electionStrategy.onNewElection(localNode, 1, f));
            assertThat(startJoinRequest.getTerm(), is(greaterThan(1L)));
        }
    }

    public void testFailsToClaimATerm() throws Exception {
        try (var fakeStatelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry())) {
            boolean failToReadRegister = randomBoolean();
            boolean failCASOperation = failToReadRegister == false || randomBoolean();
            var termLeaseBlobContainer = new FilterBlobContainer(fakeStatelessNode.objectStoreService.getTermLeaseBlobContainer()) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    return child;
                }

                @Override
                public void getRegister(String key, ActionListener<OptionalLong> listener) {
                    if (failToReadRegister) {
                        listener.onFailure(new IOException("Unable to get register value"));
                    } else {
                        super.getRegister(key, listener);
                    }
                }

                @Override
                public void compareAndSetRegister(String key, long expected, long updated, ActionListener<Boolean> listener) {
                    if (failCASOperation) {
                        listener.onFailure(new IOException("Failed CAS"));
                    } else {
                        super.compareAndSetRegister(key, expected, updated, listener);
                    }
                }
            };

            var electionStrategy = new StatelessElectionStrategy(termLeaseBlobContainer);

            var localNode = newDiscoveryNode("local-node");

            expectThrows(
                Exception.class,
                () -> PlainActionFuture.<StartJoinRequest, Exception>get(f -> electionStrategy.onNewElection(localNode, 2, f))
            );
        }
    }

    public void testElectionQuorum() throws Exception {
        try (var fakeStatelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry())) {
            var electionStrategy = new StatelessElectionStrategy(fakeStatelessNode.objectStoreService.getTermLeaseBlobContainer());
            var localNode = newDiscoveryNode("local-node");
            var anotherNode = newDiscoveryNode("another-node");

            var votingConfiguration = new CoordinationMetadata.VotingConfiguration(Set.of(localNode.getId()));

            {
                var voteCollection = new CoordinationState.VoteCollection();
                voteCollection.addJoinVote(getJoin(localNode, localNode));
                if (randomBoolean()) {
                    voteCollection.addJoinVote(getJoin(anotherNode, localNode));
                }

                assertTrue(electionStrategy.isElectionQuorum(localNode, 1, 0, 0, votingConfiguration, votingConfiguration, voteCollection));
            }

            {
                var voteCollection = new CoordinationState.VoteCollection();

                voteCollection.addJoinVote(getJoin(anotherNode, localNode));

                assertFalse(
                    electionStrategy.isElectionQuorum(localNode, 1, 0, 0, votingConfiguration, votingConfiguration, voteCollection)
                );
            }

            {
                var voteCollection = new CoordinationState.VoteCollection();
                assertFalse(
                    electionStrategy.isElectionQuorum(localNode, 1, 0, 0, votingConfiguration, votingConfiguration, voteCollection)
                );
            }
        }
    }

    public void testPublishQuorum() throws Exception {
        try (var fakeStatelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry())) {
            var electionStrategy = new StatelessElectionStrategy(fakeStatelessNode.objectStoreService.getTermLeaseBlobContainer());
            var localNode = newDiscoveryNode("local-node");

            var votingConfiguration = new CoordinationMetadata.VotingConfiguration(Set.of(localNode.getId()));

            {
                var voteCollection = new CoordinationState.VoteCollection();
                voteCollection.addVote(localNode);
                assertTrue(electionStrategy.isPublishQuorum(voteCollection, votingConfiguration, votingConfiguration));
            }

            {
                var voteCollection = new CoordinationState.VoteCollection();
                voteCollection.addVote(newDiscoveryNode("another-node"));
                assertFalse(electionStrategy.isPublishQuorum(voteCollection, votingConfiguration, votingConfiguration));
            }

            {
                var voteCollection = new CoordinationState.VoteCollection();
                assertFalse(electionStrategy.isPublishQuorum(voteCollection, votingConfiguration, votingConfiguration));
            }
        }
    }

    public void testBeforeCommitChecksIfTheTermIsStillFresh() throws Exception {
        try (var fakeStatelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry())) {
            var localNode = newDiscoveryNode("local-node");
            var electionStrategy = new StatelessElectionStrategy(fakeStatelessNode.objectStoreService.getTermLeaseBlobContainer());
            PlainActionFuture.<StartJoinRequest, Exception>get(f -> electionStrategy.onNewElection(localNode, 1, f));

            PlainActionFuture.<Void, Exception>get(f -> electionStrategy.beforeCommit(1, 1, f));

            StartJoinRequest startJoinRequest = PlainActionFuture.get(f -> electionStrategy.onNewElection(localNode, 2, f));
            assertThat(startJoinRequest.getTerm(), is(equalTo(2L)));

            expectThrows(Exception.class, () -> PlainActionFuture.<Void, Exception>get(f -> electionStrategy.beforeCommit(1, 1, f)));
        }
    }

    public void testTermsAreAssignedOncePerNode() throws Exception {
        try (var fakeStatelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry())) {
            var electionStrategy = new StatelessElectionStrategy(fakeStatelessNode.objectStoreService.getTermLeaseBlobContainer());

            var numberOfTermsGrantedBeforeStopping = 100;
            var latch = new CountDownLatch(numberOfTermsGrantedBeforeStopping);

            var numNodes = randomIntBetween(10, 15);
            var barrier = new CyclicBarrier(numNodes + 1);
            var running = new AtomicBoolean(true);
            var nodes = new ArrayList<Node>();

            for (int i = 0; i < numNodes; i++) {
                nodes.add(new Node("node-" + i, electionStrategy, running, barrier, latch::countDown));
            }

            nodes.forEach(Node::start);

            barrier.await();
            latch.await();
            running.set(false);

            nodes.forEach(Node::joinUninterruptibly);

            var sortedTermsBySeqNo = nodes.stream().flatMap(node -> node.getGrantedTerms().stream()).sorted().toList();

            var seenTerms = new HashSet<Long>();
            for (long grantedTerm : sortedTermsBySeqNo) {
                assertTrue("Term " + grantedTerm + " granted more than once", seenTerms.add(grantedTerm));
            }

            var maxTermGranted = sortedTermsBySeqNo.get(sortedTermsBySeqNo.size() - 1);
            assertThat(maxTermGranted, is(greaterThanOrEqualTo((long) numberOfTermsGrantedBeforeStopping)));
        }
    }

    static class Node extends Thread {
        private final DiscoveryNode node;
        private final AtomicBoolean running;
        private final StatelessElectionStrategy electionStrategy;
        private final CyclicBarrier startBarrier;
        private final Runnable onTermGranted;
        private final List<Long> grantedTerms = new ArrayList<>();

        Node(
            String id,
            StatelessElectionStrategy electionStrategy,
            AtomicBoolean running,
            CyclicBarrier startBarrier,
            Runnable onTermGranted
        ) {
            super(id);
            this.node = newDiscoveryNode(id);
            this.running = running;
            this.electionStrategy = electionStrategy;
            this.startBarrier = startBarrier;
            this.onTermGranted = onTermGranted;
        }

        @Override
        public void run() {
            awaitForStartBarrier();
            while (running.get()) {
                try {
                    var currentTerm = PlainActionFuture.get(electionStrategy::getCurrentLeaseTerm).orElse(0);
                    long proposedTerm = currentTerm + 1;
                    // Add some variability to the scheduling
                    sleepUninterruptibly(randomIntBetween(0, 50));
                    StartJoinRequest startJoinRequest = PlainActionFuture.get(l -> electionStrategy.onNewElection(node, proposedTerm, l));
                    grantedTerms.add(startJoinRequest.getTerm());
                    onTermGranted.run();
                } catch (CoordinationStateRejectedException e) {
                    // Another node won this term
                }
            }
        }

        List<Long> getGrantedTerms() {
            return Collections.unmodifiableList(grantedTerms);
        }

        void sleepUninterruptibly(long timeInMillis) {
            try {
                Thread.sleep(timeInMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void joinUninterruptibly() {
            try {
                join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void awaitForStartBarrier() {
            try {
                startBarrier.await();
            } catch (BrokenBarrierException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private static DiscoveryNode newDiscoveryNode(String id) {
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), emptyMap(), Set.of(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
    }

    private Join getJoin(DiscoveryNode sourceNode, DiscoveryNode targetNode) {
        return new Join(sourceNode, targetNode, 1, 0, 0);
    }
}
