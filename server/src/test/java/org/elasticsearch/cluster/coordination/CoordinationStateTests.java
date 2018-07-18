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

import org.elasticsearch.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.cluster.coordination.CoordinationState.InMemoryPersistedState;
import org.elasticsearch.cluster.coordination.CoordinationState.PersistedState;
import org.elasticsearch.cluster.coordination.Messages.ApplyCommit;
import org.elasticsearch.cluster.coordination.Messages.Join;
import org.elasticsearch.cluster.coordination.Messages.PublishRequest;
import org.elasticsearch.cluster.coordination.Messages.PublishResponse;
import org.elasticsearch.cluster.coordination.Messages.StartJoinRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Before;

import java.util.Collections;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;


public class CoordinationStateTests extends ESTestCase {

    public static CoordinationState createInitialState(PersistedState storage, DiscoveryNode localNode) {
        return new CoordinationState(Settings.EMPTY, localNode, storage);
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

    DiscoveryNode node1;
    DiscoveryNode node2;
    DiscoveryNode node3;

    ClusterState initialStateNode1;
    ClusterState initialStateNode2;
    ClusterState initialStateNode3;

    PersistedState s1;
    PersistedState s2;
    PersistedState s3;

    CoordinationState n1;
    CoordinationState n2;
    CoordinationState n3;

    @Before
    public void setupNodes() {
        node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);
        node3 = new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT);

        initialStateNode1 = clusterState(0L, 0L, node1, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 42L);
        initialStateNode2 = clusterState(0L, 0L, node2, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 42L);
        initialStateNode3 = clusterState(0L, 0L, node3, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 42L);

        assertTrue(initialStateNode1.getLastAcceptedConfiguration().isEmpty());
        assertTrue(initialStateNode1.getLastCommittedConfiguration().isEmpty());

        s1 = new InMemoryPersistedState(0L, initialStateNode1);
        s2 = new InMemoryPersistedState(0L, initialStateNode2);
        s3 = new InMemoryPersistedState(0L, initialStateNode3);

        n1 = createInitialState(s1, node1);
        n2 = createInitialState(s2, node2);
        n3 = createInitialState(s3, node2);
    }

    public void testSetInitialState() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        assertTrue(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        n1.setInitialState(state1);
        assertThat(n1.getLastAcceptedState(), equalTo(state1));
    }

    public void testSetInitialStateWhenAlreadySet() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        assertTrue(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        n1.setInitialState(state1);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> n1.setInitialState(state1)).getMessage(),
            containsString("initial state already set"));
    }

    public void testStartJoinBeforeBootstrap() {
        assertThat(n1.getCurrentTerm(), equalTo(0L));
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(randomFrom(node1, node2), randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertThat(v1.getTargetNode(), equalTo(startJoinRequest1.getSourceNode()));
        assertThat(v1.getSourceNode(), equalTo(node1));
        assertThat(v1.getTerm(), equalTo(startJoinRequest1.getTerm()));
        assertThat(v1.getLastAcceptedTerm(), equalTo(initialStateNode1.term()));
        assertThat(v1.getLastAcceptedVersion(), equalTo(initialStateNode1.version()));
        assertThat(n1.getCurrentTerm(), equalTo(startJoinRequest1.getTerm()));

        StartJoinRequest startJoinRequest2 = new StartJoinRequest(randomFrom(node1, node2),
            randomLongBetween(0, startJoinRequest1.getTerm()));
        expectThrows(CoordinationStateRejectedException.class, () -> n1.handleStartJoin(startJoinRequest2));
    }

    public void testStartJoinAfterBootstrap() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        assertTrue(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        n1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(randomFrom(node1, node2), randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertThat(v1.getTargetNode(), equalTo(startJoinRequest1.getSourceNode()));
        assertThat(v1.getSourceNode(), equalTo(node1));
        assertThat(v1.getTerm(), equalTo(startJoinRequest1.getTerm()));
        assertThat(v1.getLastAcceptedTerm(), equalTo(state1.term()));
        assertThat(v1.getLastAcceptedVersion(), equalTo(state1.version()));
        assertThat(n1.getCurrentTerm(), equalTo(startJoinRequest1.getTerm()));

        StartJoinRequest startJoinRequest2 = new StartJoinRequest(randomFrom(node1, node2),
            randomLongBetween(0, startJoinRequest1.getTerm()));
        expectThrows(CoordinationStateRejectedException.class, () -> n1.handleStartJoin(startJoinRequest2));
    }

    public void testJoinBeforeBootstrap() {
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> n1.handleJoin(v1)).getMessage(),
            containsString("initial configuration not set"));
    }

    public void testJoinWithWrongTarget() {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertThat(expectThrows(AssertionError.class, () -> n1.handleJoin(v1)).getMessage(),
            containsString("wrong node"));
    }

    public void testJoinWithBadCurrentTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        Join badJoin = new Join(randomFrom(node1, node2), node1, randomNonNegativeLong(),
            randomLongBetween(0, startJoinRequest1.getTerm() - 1), randomNonNegativeLong());
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> n1.handleJoin(badJoin)).getMessage(),
            containsString("does not match current term"));
    }

    public void testJoinWithHigherAcceptedTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        n1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        n1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = n1.handleStartJoin(startJoinRequest2);

        Join badJoin = new Join(randomFrom(node1, node2), node1, randomNonNegativeLong(),
            v1.getTerm(), randomLongBetween(state2.term() + 1, 30));
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> n1.handleJoin(badJoin)).getMessage(),
            containsString("higher than current last accepted term"));
    }

    public void testJoinWithHigherVersion() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        n1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        n1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = n1.handleStartJoin(startJoinRequest2);

        Join badJoin = new Join(randomFrom(node1, node2), node1, randomLongBetween(state2.version() + 1, 30),
            v1.getTerm(), state2.term());
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> n1.handleJoin(badJoin)).getMessage(),
            containsString("higher than current version"));
    }

    public void testJoinWinsElection() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        n1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        n1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = n1.handleStartJoin(startJoinRequest2);

        Join join = new Join(node1, node1, randomLongBetween(0, state2.version()),
            v1.getTerm(), randomLongBetween(0, state2.term()));
        assertTrue(n1.handleJoin(join));
        assertTrue(n1.electionWon());
        assertEquals(n1.getLastPublishedVersion(), n1.getLastAcceptedVersion());
        assertFalse(n1.handleJoin(join));
    }

    public void testJoinDoesNotWinElection() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        n1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        n1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = n1.handleStartJoin(startJoinRequest2);

        Join join = new Join(node2, node1, randomLongBetween(0, state2.version()),
            v1.getTerm(), randomLongBetween(0, state2.term()));
        assertTrue(n1.handleJoin(join));
        assertFalse(n1.electionWon());
        assertEquals(n1.getLastPublishedVersion(), 0L);
        assertFalse(n1.handleJoin(join));
    }

    public void testHandleClientValue() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        Join v2 = n2.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());
        assertTrue(n1.handleJoin(v2));

        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));

        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig, 42L);
        PublishRequest publishRequest = n1.handleClientValue(state2);
        assertThat(publishRequest.getAcceptedState(), equalTo(state2));
        assertThat(n1.getLastPublishedVersion(), equalTo(state2.version()));
    }

    public void testHandleClientValueWhenElectionNotWon() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        if (randomBoolean()) {
            n1.setInitialState(state1);
        }
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> n1.handleClientValue(state1)).getMessage(),
            containsString("election not won"));
    }

    public void testHandleClientValueDuringOngoingPublication() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());

        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        n1.handleClientValue(state2);

        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), 3L, node1, initialConfig, initialConfig, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> n1.handleClientValue(state3)).getMessage(),
            containsString("cannot start publishing next value before accepting previous one"));
    }

    public void testHandleClientValueWithBadTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(3, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());

        long term = randomBoolean() ?
            randomLongBetween(startJoinRequest1.getTerm() + 1, 10) :
            randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        ClusterState state2 = clusterState(term, 2L, node1, initialConfig, initialConfig, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> n1.handleClientValue(state2)).getMessage(),
            containsString("does not match current term"));
    }

    public void testHandleClientValueWithOldVersion() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());

        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 1L, node1, initialConfig, initialConfig, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> n1.handleClientValue(state2)).getMessage(),
            containsString("lower or equal to last published version"));
    }

    public void testHandleClientValueWithReconfigurationWhileAlreadyReconfiguring() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        Join v2 = n2.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());
        assertTrue(n1.handleJoin(v2));

        VotingConfiguration newConfig1 = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig1, 42L);
        PublishRequest publishRequest = n1.handleClientValue(state2);
        n1.handlePublishRequest(publishRequest);
        VotingConfiguration newConfig2 = new VotingConfiguration(Collections.singleton(node3.getId()));
        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), 3L, node1, initialConfig, newConfig2, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> n1.handleClientValue(state3)).getMessage(),
            containsString("only allow reconfiguration while not already reconfiguring"));
    }

    public void testHandleClientValueWithIllegalCommittedConfigurationChange() {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());

        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, newConfig, newConfig, 42L);
        assertThat(expectThrows(AssertionError.class, () -> n1.handleClientValue(state2)).getMessage(),
            containsString("last committed configuration should not change"));
    }

    public void testHandleClientValueWithConfigurationChangeButNoJoinQuorum() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());

        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> n1.handleClientValue(state2)).getMessage(),
            containsString("only allow reconfiguration if join quorum available for new config"));
    }

    public void testHandlePublishRequest() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        if (randomBoolean()) {
            assertTrue(n1.handleJoin(v1));
            assertTrue(n1.electionWon());
        }
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        PublishResponse publishResponse = n1.handlePublishRequest(new PublishRequest(state2));
        assertThat(publishResponse.getTerm(), equalTo(state2.term()));
        assertThat(publishResponse.getVersion(), equalTo(state2.version()));
        assertThat(n1.getLastAcceptedState(), equalTo(state2));
    }

    public void testHandlePublishRequestWithBadTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        if (randomBoolean()) {
            assertTrue(n1.handleJoin(v1));
            assertTrue(n1.electionWon());
        }
        long term = randomBoolean() ?
            randomLongBetween(startJoinRequest1.getTerm() + 1, 10) :
            randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        ClusterState state2 = clusterState(term, 2L, node1, initialConfig, initialConfig, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> n1.handlePublishRequest(new PublishRequest(state2))).getMessage(),
            containsString("does not match current term"));
    }

    public void testHandlePublishRequestWithOlderVersion() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        if (randomBoolean()) {
            assertTrue(n1.handleJoin(v1));
            assertTrue(n1.electionWon());
        }
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        n1.handlePublishRequest(new PublishRequest(state2));
        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(0, state2.version()), node1, initialConfig,
            initialConfig, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> n1.handlePublishRequest(new PublishRequest(state3))).getMessage(),
            containsString("older than current version"));
    }

    public void testHandlePublishResponseWithCommit() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        PublishRequest publishRequest = n1.handleClientValue(state2);
        PublishResponse publishResponse = n1.handlePublishRequest(publishRequest);
        Optional<ApplyCommit> applyCommit = n1.handlePublishResponse(node1, publishResponse);
        assertTrue(applyCommit.isPresent());
        assertThat(applyCommit.get().getSourceNode(), equalTo(node1));
        assertThat(applyCommit.get().getTerm(), equalTo(state2.term()));
        assertThat(applyCommit.get().getVersion(), equalTo(state2.version()));
    }

    public void testHandlePublishResponseWithoutCommit() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        PublishRequest publishRequest = n1.handleClientValue(state2);
        PublishResponse publishResponse = n1.handlePublishRequest(publishRequest);
        Optional<ApplyCommit> applyCommit = n1.handlePublishResponse(node2, publishResponse);
        assertFalse(applyCommit.isPresent());
    }

    public void testHandlePublishResponseWithBadTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        PublishResponse publishResponse = n1.handlePublishRequest(new PublishRequest(state2));
        long term = randomBoolean() ?
            randomLongBetween(startJoinRequest1.getTerm() + 1, 10) :
            randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> n1.handlePublishResponse(randomFrom(node1, node2, node3),
                new PublishResponse(publishResponse.getVersion(), term))).getMessage(),
            containsString("does not match current term"));
    }

    public void testHandlePublishResponseWithVersionMismatch() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        PublishResponse publishResponse = n1.handlePublishRequest(new PublishRequest(state2));
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> n1.handlePublishResponse(randomFrom(node1, node2, node3), publishResponse)).getMessage(),
            containsString("does not match current version"));
    }

    public void testHandleCommit() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());
        Join v2 = n2.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v2));
        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig, 7L);
        PublishRequest publishRequest = n1.handleClientValue(state2);
        PublishResponse publishResponse = n1.handlePublishRequest(publishRequest);
        n1.handlePublishResponse(node1, publishResponse);
        Optional<ApplyCommit> applyCommit = n1.handlePublishResponse(node2, publishResponse);
        assertTrue(applyCommit.isPresent());
        assertThat(n1.getLastCommittedConfiguration(), equalTo(initialConfig));
        n1.handleCommit(applyCommit.get());
        assertThat(n1.getLastCommittedConfiguration(), equalTo(newConfig));
    }

    public void testHandleCommitWithBadCurrentTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 7L);
        PublishRequest publishRequest = n1.handleClientValue(state2);
        PublishResponse publishResponse = n1.handlePublishRequest(publishRequest);
        n1.handlePublishResponse(node1, publishResponse);
        long term = randomBoolean() ?
            randomLongBetween(startJoinRequest1.getTerm() + 1, 10) :
            randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> n1.handleCommit(new ApplyCommit(node1, term, 2L))).getMessage(),
            containsString("does not match current term"));
    }

    public void testHandleCommitWithBadLastAcceptedTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> n1.handleCommit(new ApplyCommit(node1, startJoinRequest1.getTerm(), 2L))).getMessage(),
            containsString("does not match last accepted term"));
    }

    public void testHandleCommitWithBadVersion() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        n1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = n1.handleStartJoin(startJoinRequest1);
        assertTrue(n1.handleJoin(v1));
        assertTrue(n1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 7L);
        PublishRequest publishRequest = n1.handleClientValue(state2);
        n1.handlePublishRequest(publishRequest);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> n1.handleCommit(new ApplyCommit(node1, startJoinRequest1.getTerm(), randomLongBetween(3, 10)))).getMessage(),
            containsString("does not match current version"));
    }


    @TestLogging("org.elasticsearch.discovery.zen2:TRACE")
    @AwaitsFix(bugUrl = "bla")
    public void testSimpleScenario() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));

        ClusterState state1 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42);
        assertTrue(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));

        assertThat(n1.getCurrentTerm(), equalTo(0L));
        Join v1 = n1.handleStartJoin(new StartJoinRequest(node2, 1));
        assertThat(n1.getCurrentTerm(), equalTo(1L));

        assertThat(n2.getCurrentTerm(), equalTo(0L));
        Join v2 = n2.handleStartJoin(new StartJoinRequest(node2, 1));
        assertThat(n2.getCurrentTerm(), equalTo(1L));

        expectThrows(AssertionError.class, () -> n1.handleJoin(v1));
        n1.setInitialState(state1);
        n1.handleJoin(v2);

        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state2 = nextStateWithTermValueAndConfig(state1, 1, 5, newConfig);
        assertTrue(state2.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node2.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));

        expectThrows(CoordinationStateRejectedException.class, () -> n1.handleClientValue(state2));
        n1.handleJoin(v1);

        PublishRequest publishRequest2 = n1.handleClientValue(state2);

        PublishResponse n1PublishResponse = n1.handlePublishRequest(publishRequest2);
        PublishResponse n2PublishResponse = n2.handlePublishRequest(publishRequest2);
        expectThrows(CoordinationStateRejectedException.class, () -> n3.handlePublishRequest(publishRequest2));
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
        expectThrows(CoordinationStateRejectedException.class, () -> n3.handleCommit(n1Commit.get()));
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
