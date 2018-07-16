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
package org.elasticsearch.discovery.zen2;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen2.ConsensusState.InMemoryPersistedState;
import org.elasticsearch.discovery.zen2.ConsensusState.PersistedState;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.Join;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.StartJoinRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Collections;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.equalTo;


public class ConsensusStateTests extends ESTestCase {

    public static ConsensusState createInitialState(PersistedState storage, DiscoveryNode localNode) {
        return new ConsensusState(Settings.EMPTY, localNode, storage);
    }

    public static ClusterState clusterState(long term, long version, DiscoveryNode localNode, VotingConfiguration lastCommittedConfig,
                                            VotingConfiguration lastAcceptedConfig, long value) {
        return clusterState(term, version, DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build(),
            lastCommittedConfig, lastAcceptedConfig, value);
    }

    public static ClusterState clusterState(long term, long version, DiscoveryNodes discoveryNodes, VotingConfiguration lastCommittedConfig,
                                            VotingConfiguration lastAcceptedConfig, long value) {
        return setValue(ClusterState.builder(ClusterName.DEFAULT)
            .version(version)
            .term(term)
            .lastCommittedConfiguration(lastCommittedConfig)
            .lastAcceptedConfiguration(lastAcceptedConfig)
            .nodes(discoveryNodes)
            .metaData(MetaData.builder()
                .clusterUUID(UUIDs.randomBase64UUID(random()))) // generate cluster UUID deterministically for repeatable tests
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

    public static ClusterState applyPersistentSettings(ClusterState clusterState, UnaryOperator<Settings.Builder> settingsUpdate) {
        return ClusterState.builder(clusterState).metaData(
            MetaData.builder(clusterState.metaData())
                .persistentSettings(settingsUpdate.apply(Settings.builder()
                    .put(clusterState.metaData().persistentSettings()))
                    .build())
                .build())
            .build();
    }

    public static long value(ClusterState clusterState) {
        return clusterState.metaData().persistentSettings().getAsLong("value", 0L);
    }

    public void testStartJoin() {
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);
        ClusterState state0 = clusterState(0L, 0L, node1, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 42);
        assertTrue(state0.getLastAcceptedConfiguration().isEmpty());
        assertTrue(state0.getLastCommittedConfiguration().isEmpty());

        PersistedState s1 = new InMemoryPersistedState(0L, state0);
        PersistedState s2 = new InMemoryPersistedState(0L, state0);
        PersistedState s3 = new InMemoryPersistedState(0L, state0);

        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42);
        assertTrue(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));

        ConsensusState n1 = createInitialState(s1, node1);
        ConsensusState n2 = createInitialState(s2, node2);

        assertThat(n1.getCurrentTerm(), equalTo(0L));
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertThat(v1.getTargetNode(), equalTo(startJoinRequest1.getSourceNode()));
        assertThat(v1.getSourceNode(), equalTo(node1));
        assertThat(v1.getTerm(), equalTo(startJoinRequest1.getTerm()));
        assertThat(v1.getLastAcceptedTerm(), equalTo(state0.term()));
        assertThat(v1.getLastAcceptedVersion(), equalTo(state0.version()));
        assertThat(n1.getCurrentTerm(), equalTo(startJoinRequest1.getTerm()));

        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(0, startJoinRequest1.getTerm()));
        expectThrows(ConsensusMessageRejectedException.class, () -> n1.handleStartJoin(startJoinRequest2));
    }

    @TestLogging("org.elasticsearch.discovery.zen2:TRACE")
    public void testSimpleScenario() {
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node3 = new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT);
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));

        ClusterState state0 = clusterState(0L, 0L, node1, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 42);
        assertTrue(state0.getLastAcceptedConfiguration().isEmpty());
        assertTrue(state0.getLastCommittedConfiguration().isEmpty());

        PersistedState s1 = new InMemoryPersistedState(0L, state0);
        PersistedState s2 = new InMemoryPersistedState(0L, state0);
        PersistedState s3 = new InMemoryPersistedState(0L, state0);

        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42);
        assertTrue(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));

        ConsensusState n1 = createInitialState(s1, node1);
        ConsensusState n2 = createInitialState(s2, node2);
        ConsensusState n3 = createInitialState(s3, node3);

        assertThat(n1.getCurrentTerm(), equalTo(0L));
        Join v1 = n1.handleStartJoin(new StartJoinRequest(node2, 1));
        assertThat(n1.getCurrentTerm(), equalTo(1L));

        assertThat(n2.getCurrentTerm(), equalTo(0L));
        Join v2 = n2.handleStartJoin(new StartJoinRequest(node2, 1));
        assertThat(n2.getCurrentTerm(), equalTo(1L));

        expectThrows(ConsensusMessageRejectedException.class, () -> n1.handleJoin(v1));
        n1.setInitialState(state1);
        n1.handleJoin(v2);

        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state2 = nextStateWithTermValueAndConfig(state1, 1, 5, newConfig);
        assertTrue(state2.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node2.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));

        expectThrows(ConsensusMessageRejectedException.class, () -> n1.handleClientValue(state2));
        n1.handleJoin(v1);

        PublishRequest publishRequest2 = n1.handleClientValue(state2);

        PublishResponse n1PublishResponse = n1.handlePublishRequest(publishRequest2);
        PublishResponse n2PublishResponse = n2.handlePublishRequest(publishRequest2);
        expectThrows(ConsensusMessageRejectedException.class, () -> n3.handlePublishRequest(publishRequest2));
        n3.handleStartJoin(new StartJoinRequest(node2, 1));

        assertFalse(n1.handlePublishResponse(node1, n1PublishResponse).isPresent());
        Optional<ApplyCommit> n1Commit = n1.handlePublishResponse(node2, n2PublishResponse);
        assertTrue(n1Commit.isPresent());

        assertThat(n1.getLastAcceptedVersion(), equalTo(2L));
        assertThat(n1.getLastCommittedConfiguration(), equalTo(initialConfig));
        assertThat(n1.getLastAcceptedConfiguration(), equalTo(newConfig));
        n1.handleCommit(n1Commit.get());
        assertThat(n1.getLastAcceptedVersion(), equalTo(2L));
        assertThat(n1.getLastCommittedConfiguration(), equalTo(newConfig));

        assertThat(n3.getLastAcceptedVersion(), equalTo(0L));
        expectThrows(ConsensusMessageRejectedException.class, () -> n3.handleCommit(n1Commit.get()));
        assertThat(n3.getLastAcceptedVersion(), equalTo(0L));

        assertThat(n2.getLastAcceptedVersion(), equalTo(2L));
        assertThat(value(n2.getLastAcceptedState()), equalTo(5L));
        assertThat(n2.getLastCommittedConfiguration(), equalTo(initialConfig));
        n2.handleCommit(n1Commit.get());
        assertThat(n2.getLastAcceptedVersion(), equalTo(2L));
        assertThat(value(n2.getLastAcceptedState()), equalTo(5L));
        assertThat(n2.getLastCommittedConfiguration(), equalTo(newConfig));
    }

    static ClusterState nextStateWithTermValueAndConfig(ClusterState lastState, long term, long newValue, VotingConfiguration newConfig) {
        return clusterState(term, lastState.version() + 1, lastState.nodes(),
            lastState.getLastCommittedConfiguration(), newConfig, newValue);
    }

    static ClusterState nextStateWithValue(ClusterState lastState, long newValue) {
        return clusterState(lastState.term(), lastState.version() + 1, lastState.nodes(),
            lastState.getLastCommittedConfiguration(), lastState.getLastAcceptedConfiguration(),
            newValue);
    }

}
