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

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.RandomizedTest.rarely;
import static java.util.stream.Collectors.toSet;
import static org.apache.lucene.util.LuceneTestCase.random;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.test.ESTestCase.randomSubsetOf;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public class CoordinationStateTestCluster {

    public static ClusterState clusterState(long term, long version, DiscoveryNode localNode,
                                            CoordinationMetaData.VotingConfiguration lastCommittedConfig,
                                            CoordinationMetaData.VotingConfiguration lastAcceptedConfig, long value) {
        return clusterState(term, version, DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build(),
            lastCommittedConfig, lastAcceptedConfig, value);
    }

    public static ClusterState clusterState(long term, long version, DiscoveryNodes discoveryNodes,
                                            CoordinationMetaData.VotingConfiguration lastCommittedConfig,
                                            CoordinationMetaData.VotingConfiguration lastAcceptedConfig, long value) {
        return setValue(ClusterState.builder(ClusterName.DEFAULT)
            .version(version)
            .nodes(discoveryNodes)
            .metaData(MetaData.builder()
                .clusterUUID(UUIDs.randomBase64UUID(random())) // generate cluster UUID deterministically for repeatable tests
                .coordinationMetaData(CoordinationMetaData.builder()
                    .term(term)
                    .lastCommittedConfiguration(lastCommittedConfig)
                    .lastAcceptedConfiguration(lastAcceptedConfig)
                    .build()))
            .stateUUID(UUIDs.randomBase64UUID(random())) // generate cluster state UUID deterministically for repeatable tests
            .build(), value);
    }

    public static ClusterState setValue(ClusterState clusterState, long value) {
        return ClusterState.builder(clusterState).metaData(
            MetaData.builder(clusterState.metaData())
                .persistentSettings(Settings.builder()
                    .put(clusterState.metaData().persistentSettings())
                    .put("value", value)
                    .build())
                .build())
            .build();
    }

    public static long value(ClusterState clusterState) {
        return clusterState.metaData().persistentSettings().getAsLong("value", 0L);
    }

    static class ClusterNode {
        private final ElectionStrategy electionStrategy;

        DiscoveryNode localNode;
        CoordinationState.PersistedState persistedState;
        CoordinationState state;

        ClusterNode(DiscoveryNode localNode, ElectionStrategy electionStrategy) {
            this.localNode = localNode;
            persistedState = new InMemoryPersistedState(0L,
                clusterState(0L, 0L, localNode, CoordinationMetaData.VotingConfiguration.EMPTY_CONFIG,
                    CoordinationMetaData.VotingConfiguration.EMPTY_CONFIG, 0L));
            this.electionStrategy = electionStrategy;
            state = new CoordinationState(localNode, persistedState, electionStrategy);
        }

        void reboot() {
            if (localNode.isMasterNode() == false && rarely()) {
                // master-ineligible nodes can't be trusted to persist the cluster state properly
                persistedState = new InMemoryPersistedState(0L,
                    clusterState(0L, 0L, localNode, CoordinationMetaData.VotingConfiguration.EMPTY_CONFIG,
                        CoordinationMetaData.VotingConfiguration.EMPTY_CONFIG, 0L));
            }

            final Set<DiscoveryNodeRole> roles = new HashSet<>(localNode.getRoles());
            if (randomBoolean()) {
                if (roles.contains(DiscoveryNodeRole.MASTER_ROLE)) {
                    roles.remove(DiscoveryNodeRole.MASTER_ROLE);
                } else {
                    roles.add(DiscoveryNodeRole.MASTER_ROLE);
                }
            }

            localNode = new DiscoveryNode(localNode.getName(), localNode.getId(), UUIDs.randomBase64UUID(random()),
                localNode.getHostName(), localNode.getHostAddress(), localNode.getAddress(), localNode.getAttributes(),
                roles, localNode.getVersion());

            state = new CoordinationState(localNode, persistedState, electionStrategy);
        }

        void setInitialState(CoordinationMetaData.VotingConfiguration initialConfig, long initialValue) {
            final ClusterState.Builder builder = ClusterState.builder(state.getLastAcceptedState());
            builder.metaData(MetaData.builder()
                .coordinationMetaData(CoordinationMetaData.builder()
                    .lastAcceptedConfiguration(initialConfig)
                    .lastCommittedConfiguration(initialConfig)
                    .build()));
            state.setInitialState(setValue(builder.build(), initialValue));
        }
    }

    final ElectionStrategy electionStrategy;
    final List<Message> messages;
    final List<ClusterNode> clusterNodes;
    final CoordinationMetaData.VotingConfiguration initialConfiguration;
    final long initialValue;

    CoordinationStateTestCluster(List<DiscoveryNode> nodes, ElectionStrategy electionStrategy) {
        this.electionStrategy = electionStrategy;
        messages = new ArrayList<>();

        clusterNodes = nodes.stream()
            .map(node -> new ClusterNode(node, electionStrategy))
            .collect(Collectors.toList());

        initialConfiguration = randomVotingConfig();
        initialValue = randomLong();
    }

    static class Message {
        final DiscoveryNode sourceNode;
        final DiscoveryNode targetNode;
        final Object payload;

        Message(DiscoveryNode sourceNode, DiscoveryNode targetNode, Object payload) {
            this.sourceNode = sourceNode;
            this.targetNode = targetNode;
            this.payload = payload;
        }
    }

    void reply(Message m, Object payload) {
        messages.add(new Message(m.targetNode, m.sourceNode, payload));
    }

    void broadcast(DiscoveryNode sourceNode, Object payload) {
        messages.addAll(clusterNodes.stream().map(cn -> new Message(sourceNode, cn.localNode, payload)).collect(Collectors.toList()));
    }

    Optional<ClusterNode> getNode(DiscoveryNode node) {
        return clusterNodes.stream().filter(cn -> cn.localNode.equals(node)).findFirst();
    }

    CoordinationMetaData.VotingConfiguration randomVotingConfig() {
        return new CoordinationMetaData.VotingConfiguration(
            randomSubsetOf(randomIntBetween(1, clusterNodes.size()), clusterNodes).stream()
                .map(cn -> cn.localNode.getId()).collect(toSet()));
    }

    void applyMessage(Message message) {
        final Optional<ClusterNode> maybeNode = getNode(message.targetNode);
        if (maybeNode.isPresent() == false) {
            throw new CoordinationStateRejectedException("node not available");
        } else {
            final Object payload = message.payload;
            if (payload instanceof StartJoinRequest) {
                reply(message, maybeNode.get().state.handleStartJoin((StartJoinRequest) payload));
            } else if (payload instanceof Join) {
                maybeNode.get().state.handleJoin((Join) payload);
            } else if (payload instanceof PublishRequest) {
                reply(message, maybeNode.get().state.handlePublishRequest((PublishRequest) payload));
            } else if (payload instanceof PublishResponse) {
                maybeNode.get().state.handlePublishResponse(message.sourceNode, (PublishResponse) payload)
                    .ifPresent(ac -> broadcast(message.targetNode, ac));
            } else if (payload instanceof ApplyCommitRequest) {
                maybeNode.get().state.handleCommit((ApplyCommitRequest) payload);
            } else {
                throw new AssertionError("unknown message type");
            }
        }
    }

    void runRandomly() {
        final int iterations = 10000;
        final long maxTerm = 4;
        long nextTerm = 1;
        for (int i = 0; i < iterations; i++) {
            try {
                if (rarely() && nextTerm < maxTerm) {
                    final long term = rarely() ? randomLongBetween(0, maxTerm + 1) : nextTerm++;
                    final StartJoinRequest startJoinRequest = new StartJoinRequest(randomFrom(clusterNodes).localNode, term);
                    broadcast(startJoinRequest.getSourceNode(), startJoinRequest);
                } else if (rarely()) {
                    randomFrom(clusterNodes).setInitialState(initialConfiguration, initialValue);
                } else if (rarely() && rarely()) {
                    randomFrom(clusterNodes).reboot();
                } else if (rarely()) {
                    final List<ClusterNode> masterNodes = clusterNodes.stream().filter(cn -> cn.state.electionWon())
                        .collect(Collectors.toList());
                    if (masterNodes.isEmpty() == false) {
                        final ClusterNode clusterNode = randomFrom(masterNodes);
                        final long term = rarely() ? randomLongBetween(0, maxTerm + 1) : clusterNode.state.getCurrentTerm();
                        final long version = rarely() ? randomIntBetween(0, 5) : clusterNode.state.getLastPublishedVersion() + 1;
                        final CoordinationMetaData.VotingConfiguration acceptedConfig = rarely() ? randomVotingConfig() :
                            clusterNode.state.getLastAcceptedConfiguration();
                        final PublishRequest publishRequest = clusterNode.state.handleClientValue(
                            clusterState(term, version, clusterNode.localNode, clusterNode.state.getLastCommittedConfiguration(),
                                acceptedConfig, randomLong()));
                        broadcast(clusterNode.localNode, publishRequest);
                    }
                } else if (messages.isEmpty() == false) {
                    applyMessage(randomFrom(messages));
                }

                // check node invariants after each iteration
                clusterNodes.forEach(cn -> cn.state.invariant());
            } catch (CoordinationStateRejectedException e) {
                // ignore
            }
        }

        // check system invariants. It's sufficient to do this at the end as these invariants are monotonic.
        invariant();
    }

    void invariant() {
        // one master per term
        messages.stream().filter(m -> m.payload instanceof PublishRequest)
            .collect(Collectors.groupingBy(m -> ((PublishRequest) m.payload).getAcceptedState().term()))
            .forEach((term, publishMessages) -> {
                Set<DiscoveryNode> mastersForTerm = publishMessages.stream().collect(Collectors.groupingBy(m -> m.sourceNode)).keySet();
                assertThat("Multiple masters " + mastersForTerm + " for term " + term, mastersForTerm, hasSize(1));
            });

        // unique cluster state per (term, version) pair
        messages.stream().filter(m -> m.payload instanceof PublishRequest)
            .map(m -> ((PublishRequest) m.payload).getAcceptedState())
            .collect(Collectors.groupingBy(ClusterState::term))
            .forEach((term, clusterStates) -> {
                clusterStates.stream().collect(Collectors.groupingBy(ClusterState::version))
                    .forEach((version, clusterStates1) -> {
                        Set<String> clusterStateUUIDsForTermAndVersion = clusterStates1.stream().collect(Collectors.groupingBy(
                            ClusterState::stateUUID
                        )).keySet();
                        assertThat("Multiple cluster states " + clusterStates1 + " for term " + term + " and version " + version,
                            clusterStateUUIDsForTermAndVersion, hasSize(1));

                        Set<Long> clusterStateValuesForTermAndVersion = clusterStates1.stream().collect(Collectors.groupingBy(
                            CoordinationStateTestCluster::value
                        )).keySet();

                        assertThat("Multiple cluster states " + clusterStates1 + " for term " + term + " and version " + version,
                            clusterStateValuesForTermAndVersion, hasSize(1));
                    });
            });
    }
}
