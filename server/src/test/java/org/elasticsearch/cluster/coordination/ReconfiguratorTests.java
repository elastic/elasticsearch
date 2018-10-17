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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.coordination.Reconfigurator.CLUSTER_MASTER_NODES_FAILURE_TOLERANCE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ReconfiguratorTests extends ESTestCase {

    public void testReconfigurationExamples() {
        for (int masterNodesFailureTolerance = 0; masterNodesFailureTolerance <= 2; masterNodesFailureTolerance++) {
            check(nodes("a"), conf("a"), masterNodesFailureTolerance, conf("a"));
            check(nodes("a", "b"), conf("a"), masterNodesFailureTolerance, conf("a"));
            check(nodes("a", "b", "c"), conf("a"), masterNodesFailureTolerance, conf("a", "b", "c"));
            check(nodes("a", "b", "c"), conf("a", "b"), masterNodesFailureTolerance, conf("a", "b", "c"));
            check(nodes("a", "b", "c"), conf("a", "b", "c"), masterNodesFailureTolerance, conf("a", "b", "c"));
            check(nodes("a", "b", "c", "d"), conf("a", "b", "c"), masterNodesFailureTolerance, conf("a", "b", "c"));
            check(nodes("a", "b", "c", "d", "e"), conf("a", "b", "c"), masterNodesFailureTolerance, conf("a", "b", "c", "d", "e"));
            check(nodes("a", "b"), conf("a", "b", "e"), masterNodesFailureTolerance,
                masterNodesFailureTolerance == 1 ? conf("a", "b", "e") : conf("a"));
            check(nodes("a", "b", "c"), conf("a", "b", "e"), masterNodesFailureTolerance, conf("a", "b", "c"));
            check(nodes("a", "b", "c", "d"), conf("a", "b", "e"), masterNodesFailureTolerance, conf("a", "b", "c"));
            check(nodes("a", "b", "c", "d", "e"), conf("a", "f", "g"), masterNodesFailureTolerance, conf("a", "b", "c", "d", "e"));
            check(nodes("a", "b", "c", "d"), conf("a", "b", "c", "d", "e"), masterNodesFailureTolerance,
                masterNodesFailureTolerance == 2 ? conf("a", "b", "c", "d", "e") : conf("a", "b", "c"));
        }

        // Retiring a single node shifts the votes elsewhere if possible.
        check(nodes("a", "b"), retired("a"), conf("a"), 0, conf("b"));

        // If the safety level was never reached then retirement can take place
        check(nodes("a", "b"), retired("a"), conf("a"), 1, conf("b"));
        check(nodes("a", "b"), retired("a"), conf("b"), 1, conf("b"));

        // Retiring a node from a three-node cluster drops down to a one-node configuration if "fragile"
        check(nodes("a", "b", "c"), retired("a"), conf("a"), 0, conf("b"));
        check(nodes("a", "b", "c"), retired("a"), conf("a", "b", "c"), 0, conf("b"));

        // Retiring is prevented in a three-node cluster if "recommended"
        check(nodes("a", "b", "c"), retired("a"), conf("a", "b", "c"), 1, conf("a", "b", "c"));

        // 7 nodes, one for each combination of live/retired/current. Ideally we want the config to be the non-retired live nodes.
        // Since there are 2 non-retired live nodes we round down to 1 and just use the one that's already in the config.
        check(nodes("a", "b", "c", "f"), retired("c", "e", "f", "g"), conf("a", "c", "d", "e"), 0, conf("a"));
        // If we want the config to be at least 3 nodes then we don't retire "c" just yet.
        check(nodes("a", "b", "c", "f"), retired("c", "e", "f", "g"), conf("a", "c", "d", "e"), 1, conf("a", "b", "c"));
        // The current config never reached 5 nodes so retirement is allowed
        check(nodes("a", "b", "c", "f"), retired("c", "e", "f", "g"), conf("a", "c", "d", "e"), 2, conf("a"));
    }

    public void testReconfigurationProperty() {
        final String[] allNodes = new String[]{"a", "b", "c", "d", "e", "f", "g"};

        final String[] liveNodes = new String[randomIntBetween(1, allNodes.length)];
        randomSubsetOf(liveNodes.length, allNodes).toArray(liveNodes);

        final String[] initialVotingNodes = new String[randomIntBetween(1, allNodes.length)];
        randomSubsetOf(initialVotingNodes.length, allNodes).toArray(initialVotingNodes);

        final int masterNodesFailureTolerance
            = randomFrom(0, 1, 2);

        final Reconfigurator reconfigurator = makeReconfigurator(
            Settings.builder().put(CLUSTER_MASTER_NODES_FAILURE_TOLERANCE.getKey(), masterNodesFailureTolerance).build());
        final Set<DiscoveryNode> liveNodesSet = nodes(liveNodes);
        final ClusterState.VotingConfiguration initialConfig = conf(initialVotingNodes);

        // min configuration size comes from CLUSTER_MASTER_NODES_FAILURE_TOLERANCE as long as there are enough nodes in the current config

        if (initialConfig.getNodeIds().size() >= masterNodesFailureTolerance * 2 + 1) {
            // actual size of a quorum: half the configured nodes, which is all the live nodes plus maybe some dead ones to make up numbers
            final int quorumSize = Math.max(liveNodes.length / 2 + 1, masterNodesFailureTolerance + 1);

            final ClusterState.VotingConfiguration finalConfig = reconfigurator.reconfigure(liveNodesSet, emptySet(), initialConfig);

            final String description = "reconfigure " + liveNodesSet + " from " + initialConfig + " with failure tolerance of "
                + masterNodesFailureTolerance + " yielded " + finalConfig;

            if (quorumSize > liveNodes.length) {
                assertFalse(description + " without a live quorum", finalConfig.hasQuorum(Arrays.asList(liveNodes)));
            } else {
                final List<String> expectedQuorum = randomSubsetOf(quorumSize, liveNodes);
                assertTrue(description + " with quorum[" + quorumSize + "] of " + expectedQuorum, finalConfig.hasQuorum(expectedQuorum));
            }
        } else {
            assertThat(expectThrows(AssertionError.class,
                () -> reconfigurator.reconfigure(liveNodesSet, emptySet(), initialConfig)).getMessage(),
                containsString("does not satisfy masterNodesFailureTolerance"));
        }
    }

    private ClusterState.VotingConfiguration conf(String... nodes) {
        return new ClusterState.VotingConfiguration(Sets.newHashSet(nodes));
    }

    private Set<DiscoveryNode> nodes(String... nodes) {
        final Set<DiscoveryNode> liveNodes = new HashSet<>();
        for (String id : nodes) {
            liveNodes.add(new DiscoveryNode(id, buildNewFakeTransportAddress(), Version.CURRENT));
        }
        return liveNodes;
    }

    private Set<String> retired(String... nodes) {
        return Arrays.stream(nodes).collect(Collectors.toSet());
    }

    private void check(Set<DiscoveryNode> liveNodes, ClusterState.VotingConfiguration config, int masterNodesFailureTolerance,
                       ClusterState.VotingConfiguration expectedConfig) {
        check(liveNodes, retired(), config, masterNodesFailureTolerance, expectedConfig);
    }

    private void check(Set<DiscoveryNode> liveNodes, Set<String> retired, ClusterState.VotingConfiguration config,
                       int masterNodesFailureTolerance, ClusterState.VotingConfiguration expectedConfig) {
        final Reconfigurator reconfigurator = makeReconfigurator(Settings.builder()
            .put(CLUSTER_MASTER_NODES_FAILURE_TOLERANCE.getKey(), masterNodesFailureTolerance)
            .build());

        if (config.getNodeIds().size() < 2 * masterNodesFailureTolerance + 1) {
            assertThat(expectThrows(AssertionError.class, () -> reconfigurator.reconfigure(liveNodes, retired, config)).getMessage(),
                containsString("does not satisfy masterNodesFailureTolerance"));
        } else {
            final ClusterState.VotingConfiguration adaptedConfig = reconfigurator.reconfigure(liveNodes, retired, config);
            assertEquals(new ParameterizedMessage("[liveNodes={}, retired={}, config={}, masterNodesFailureTolerance={}]",
                    liveNodes, retired, config, masterNodesFailureTolerance).getFormattedMessage(),
                expectedConfig, adaptedConfig);
        }
    }

    private Reconfigurator makeReconfigurator(Settings settings) {
        return new Reconfigurator(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public void testDynamicSetting() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final Reconfigurator reconfigurator = new Reconfigurator(Settings.EMPTY, clusterSettings);
        final VotingConfiguration initialConfig = conf("a", "b", "c", "d", "e");

        // default is "0"
        assertThat(reconfigurator.reconfigure(nodes("a"), retired(), initialConfig), equalTo(conf("a")));

        // update to "2"
        clusterSettings.applySettings(Settings.builder().put(CLUSTER_MASTER_NODES_FAILURE_TOLERANCE.getKey(), "2").build());
        assertThat(reconfigurator.reconfigure(nodes("a"), retired(), initialConfig), sameInstance(initialConfig)); // cannot reconfigure
        assertThat(reconfigurator.reconfigure(nodes("a", "b", "c"), retired(), initialConfig), equalTo(conf("a", "b", "c", "d", "e")));

        // update to "1"
        clusterSettings.applySettings(Settings.builder().put(CLUSTER_MASTER_NODES_FAILURE_TOLERANCE.getKey(), "1").build());
        assertThat(reconfigurator.reconfigure(nodes("a"), retired(), initialConfig), sameInstance(initialConfig)); // cannot reconfigure
        assertThat(reconfigurator.reconfigure(nodes("a", "b", "c"), retired(), initialConfig), equalTo(conf("a", "b", "c")));

        // explicitly set to "0"
        clusterSettings.applySettings(Settings.builder().put(CLUSTER_MASTER_NODES_FAILURE_TOLERANCE.getKey(), "0").build());
        assertThat(reconfigurator.reconfigure(nodes("a"), retired(), initialConfig), equalTo(conf("a")));

        expectThrows(IllegalArgumentException.class, () ->
            clusterSettings.applySettings(Settings.builder().put(CLUSTER_MASTER_NODES_FAILURE_TOLERANCE.getKey(), "-1").build()));
    }
}
