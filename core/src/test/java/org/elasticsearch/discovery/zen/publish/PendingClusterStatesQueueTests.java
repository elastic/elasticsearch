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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.discovery.zen.publish.PendingClusterStatesQueue.ClusterStateContext;
import org.elasticsearch.test.ESTestCase;

import java.util.*;

import static org.hamcrest.Matchers.*;

public class PendingClusterStatesQueueTests extends ESTestCase {

    public void testSelectNextStateToProcess_empty() {
        PendingClusterStatesQueue queue = new PendingClusterStatesQueue(logger, randomIntBetween(1, 200));
        assertThat(queue.getNextClusterStateToProcess(), nullValue());
    }

    public void testDroppingStatesAtCapacity() {
        List<ClusterState> states = randomStates(scaledRandomIntBetween(10, 300), "master1", "master2", "master3", "master4");
        Collections.shuffle(states, random());
        // insert half of the states
        final int numberOfStateToDrop = states.size() / 2;
        List<ClusterState> stateToDrop = states.subList(0, numberOfStateToDrop);
        final int queueSize = states.size() - numberOfStateToDrop;
        PendingClusterStatesQueue queue = createQueueWithStates(stateToDrop, queueSize);
        List<ClusterStateContext> committedContexts = randomCommitStates(queue);
        for (ClusterState state : states.subList(numberOfStateToDrop, states.size())) {
            queue.addPending(state);
        }

        assertThat(queue.pendingClusterStates().length, equalTo(queueSize));
        // check all committed states got a failure due to the drop
        for (ClusterStateContext context : committedContexts) {
            assertThat(((MockListener) context.listener).failure, notNullValue());
        }

        // all states that should have dropped are indeed dropped.
        for (ClusterState state : stateToDrop) {
            assertThat(queue.findState(state.stateUUID()), nullValue());
        }

    }

    public void testSimpleQueueSameMaster() {
        final int numUpdates = scaledRandomIntBetween(50, 100);
        List<ClusterState> states = randomStates(numUpdates, "master");
        Collections.shuffle(states, random());
        PendingClusterStatesQueue queue;
        queue = createQueueWithStates(states);

        // no state is committed yet
        assertThat(queue.getNextClusterStateToProcess(), nullValue());

        ClusterState highestCommitted = null;
        for (ClusterStateContext context : randomCommitStates(queue)) {
            if (highestCommitted == null || context.state.supersedes(highestCommitted)) {
                highestCommitted = context.state;
            }
        }

        assertThat(queue.getNextClusterStateToProcess(), sameInstance(highestCommitted));

        queue.markAsProcessed(highestCommitted);

        // now there is nothing more to process
        assertThat(queue.getNextClusterStateToProcess(), nullValue());
    }

    public void testProcessedStateCleansStatesFromOtherMasters() {
        List<ClusterState> states = randomStates(scaledRandomIntBetween(10, 300), "master1", "master2", "master3", "master4");
        PendingClusterStatesQueue queue = createQueueWithStates(states);
        List<ClusterStateContext> committedContexts = randomCommitStates(queue);
        ClusterState randomCommitted = randomFrom(committedContexts).state;
        queue.markAsProcessed(randomCommitted);
        final String processedMaster = randomCommitted.nodes().masterNodeId();

        // now check that queue doesn't contain anything pending from another master
        for (ClusterStateContext context : queue.pendingStates) {
            final String pendingMaster = context.state.nodes().masterNodeId();
            assertThat("found a cluster state from [" + pendingMaster
                            + "], after a state from [" + processedMaster + "] was proccessed",
                    pendingMaster, equalTo(processedMaster));
        }
        // and check all committed contexts from another master were failed
        for (ClusterStateContext context : committedContexts) {
            if (context.state.nodes().masterNodeId().equals(processedMaster) == false) {
                assertThat(((MockListener) context.listener).failure, notNullValue());
            }
        }
    }

    public void testFailedStateCleansSupersededStatesOnly() {
        List<ClusterState> states = randomStates(scaledRandomIntBetween(10, 50), "master1", "master2", "master3", "master4");
        PendingClusterStatesQueue queue = createQueueWithStates(states);
        List<ClusterStateContext> committedContexts = randomCommitStates(queue);
        ClusterState toFail = randomFrom(committedContexts).state;
        queue.markAsFailed(toFail, new ElasticsearchException("boo!"));
        final Map<String, ClusterStateContext> committedContextsById = new HashMap<>();
        for (ClusterStateContext context : committedContexts) {
            committedContextsById.put(context.stateUUID(), context);
        }

        // now check that queue doesn't contain superseded states
        for (ClusterStateContext context : queue.pendingStates) {
            if (context.committed()) {
                assertFalse("found a committed cluster state, which is superseded by a failed state.\nFound:" + context.state + "\nfailed:" + toFail,
                        toFail.supersedes(context.state));
            }
        }
        // check no state has been erroneously removed
        for (ClusterState state : states) {
            ClusterStateContext pendingContext = queue.findState(state.stateUUID());
            if (pendingContext != null) {
                continue;
            }
            if (state.equals(toFail)) {
                continue;
            }
            assertThat("non-committed states should never be removed", committedContextsById, hasKey(state.stateUUID()));
            final ClusterStateContext context = committedContextsById.get(state.stateUUID());
            assertThat("removed state is not superseded by failed state. \nRemoved state:" + context + "\nfailed: " + toFail,
                    toFail.supersedes(context.state), equalTo(true));
            assertThat("removed state was failed with wrong exception", ((MockListener) context.listener).failure, notNullValue());
            assertThat("removed state was failed with wrong exception", ((MockListener) context.listener).failure.getMessage(), containsString("boo"));
        }
    }

    public void testFailAllAndClear() {
        List<ClusterState> states = randomStates(scaledRandomIntBetween(10, 50), "master1", "master2", "master3", "master4");
        PendingClusterStatesQueue queue = createQueueWithStates(states);
        List<ClusterStateContext> committedContexts = randomCommitStates(queue);
        queue.failAllStatesAndClear(new ElasticsearchException("boo!"));
        assertThat(queue.pendingStates, empty());
        assertThat(queue.getNextClusterStateToProcess(), nullValue());
        for (ClusterStateContext context : committedContexts) {
            assertThat("state was failed with wrong exception", ((MockListener) context.listener).failure, notNullValue());
            assertThat("state was failed with wrong exception", ((MockListener) context.listener).failure.getMessage(), containsString("boo"));
        }
    }

    protected List<ClusterStateContext> randomCommitStates(PendingClusterStatesQueue queue) {
        List<ClusterStateContext> committedContexts = new ArrayList<>();
        for (int iter = randomInt(queue.pendingStates.size() - 1); iter >= 0; iter--) {
            ClusterState state = queue.markAsCommitted(randomFrom(queue.pendingStates).stateUUID(), new MockListener());
            if (state != null) {
                // null cluster state means we committed twice
                committedContexts.add(queue.findState(state.stateUUID()));
            }
        }
        return committedContexts;
    }

    PendingClusterStatesQueue createQueueWithStates(List<ClusterState> states) {
        return createQueueWithStates(states, states.size() * 2); // we don't care about limits (there are dedicated tests for that)
    }

    PendingClusterStatesQueue createQueueWithStates(List<ClusterState> states, int maxQueueSize) {
        PendingClusterStatesQueue queue;
        queue = new PendingClusterStatesQueue(logger, maxQueueSize);
        for (ClusterState state : states) {
            queue.addPending(state);
        }
        return queue;
    }

    List<ClusterState> randomStates(int count, String... masters) {
        ArrayList<ClusterState> states = new ArrayList<>(count);
        ClusterState[] lastClusterStatePerMaster = new ClusterState[masters.length];
        for (; count > 0; count--) {
            int masterIndex = randomInt(masters.length - 1);
            ClusterState state = lastClusterStatePerMaster[masterIndex];
            if (state == null) {
                state = ClusterState.builder(ClusterName.DEFAULT).nodes(DiscoveryNodes.builder()
                                .put(new DiscoveryNode(masters[masterIndex], DummyTransportAddress.INSTANCE, Version.CURRENT)).masterNodeId(masters[masterIndex]).build()
                ).build();
            } else {
                state = ClusterState.builder(state).incrementVersion().build();
            }
            states.add(state);
            lastClusterStatePerMaster[masterIndex] = state;
        }
        return states;
    }

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
