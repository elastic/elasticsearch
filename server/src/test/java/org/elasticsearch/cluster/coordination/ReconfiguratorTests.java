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
import static org.elasticsearch.cluster.coordination.Reconfigurator.MINIMUM_VOTING_MASTER_NODES_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ReconfiguratorTests extends ESTestCase {

    public void testReconfigurationExamples() {
        check(nodes("a"), conf("a"), randomIntBetween(1, 10), conf("a"));
        check(nodes("a", "b"), conf("a"), 1, conf("a"));
        check(nodes("a", "b"), conf("a"), randomIntBetween(2, 10), conf("a", "b"));
        check(nodes("a", "b", "c"), conf("a"), randomIntBetween(1, 10), conf("a", "b", "c"));
        check(nodes("a", "b", "c"), conf("a", "b"), randomIntBetween(1, 10), conf("a", "b", "c"));
        check(nodes("a", "b", "c"), conf("a", "b", "c"), randomIntBetween(1, 10), conf("a", "b", "c"));
        check(nodes("a", "b", "c", "d"), conf("a", "b", "c"), randomIntBetween(1, 2), conf("a", "b", "c"));
        check(nodes("a", "b", "c", "d"), conf("a", "b", "c"), randomIntBetween(3, 10), conf("a", "b", "c", "d"));
        check(nodes("a", "b", "c", "d", "e"), conf("a", "b", "c"), randomIntBetween(1, 10), conf("a", "b", "c", "d", "e"));
        check(nodes("a", "b"), conf("a", "b", "e"), 1, conf("a"));
        check(nodes("a", "b"), conf("a", "b", "e"), randomIntBetween(2, 10), conf("a", "b", "e"));
        check(nodes("a", "b", "c"), conf("a", "b", "e"), randomIntBetween(1, 2), conf("a", "b", "c"));
        check(nodes("a", "b", "c"), conf("a", "b", "e"), randomIntBetween(3, 10), conf("a", "b", "c", "e"));
        check(nodes("a", "b", "c", "d"), conf("a", "b", "e"), randomIntBetween(1, 2), conf("a", "b", "c"));
        check(nodes("a", "b", "c", "d"), conf("a", "b", "e"), randomIntBetween(3, 10), conf("a", "b", "c", "d", "e"));
        check(nodes("a", "b", "c", "d", "e"), conf("a", "f", "g"), randomIntBetween(1, 3), conf("a", "b", "c", "d", "e"));
        check(nodes("a", "b", "c", "d", "e"), conf("a", "f", "g"), randomIntBetween(4, 10), conf("a", "b", "c", "d", "e", "f", "g"));

        // Retiring a single node shifts the votes elsewhere if possible.
        check(nodes("a", "b"), retired("a"), conf("a"), 1, conf("b"));
        check(nodes("a", "b"), retired("a"), conf("a"), 2, conf("a", "b")); // below min voter size, so no retirement takes place
        check(nodes("a", "b"), retired("a"), conf("b"), 2, conf("b"));
        check(nodes("a", "b", "c"), retired("a"), conf("a"), 1, conf("b"));
        check(nodes("a", "b", "c"), retired("a"), conf("a", "b", "c"), 1, conf("b"));
        check(nodes("a", "b", "c"), retired("a"), conf("a", "b", "c"), 2, conf("a", "b", "c"));

        // 7 nodes, one for each combination of live/retired/current. Ideally want the config to be the non-retired live nodes.
        // Since there are 2 non-retired live nodes we round down to 1 and just use the one that's already in the config.
        check(nodes("a", "b", "c", "f"), retired("c", "e", "f", "g"), conf("a", "c", "d", "e"), 1, conf("a"));
        // If we want the config to be at least 3 nodes then we don't retire "c" just yet.
        check(nodes("a", "b", "c", "f"), retired("c", "e", "f", "g"), conf("a", "c", "d", "e"), 2,
            conf("a", "b", "c"));
        // If we want the config to be at least 5 nodes then we keep the old config and add the new node "b" to it.
        check(nodes("a", "b", "c", "f"), retired("c", "e", "f", "g"), conf("a", "c", "d", "e"), randomIntBetween(3, 10),
            conf("a", "b", "c", "d", "e"));
    }

    public void testReconfigurationProperty() {
        final String[] allNodes = new String[]{"a", "b", "c", "d", "e", "f", "g"};

        final String[] liveNodes = new String[randomIntBetween(1, allNodes.length)];
        randomSubsetOf(liveNodes.length, allNodes).toArray(liveNodes);

        final String[] initialVotingNodes = new String[randomIntBetween(1, allNodes.length)];
        randomSubsetOf(initialVotingNodes.length, allNodes).toArray(initialVotingNodes);

        final int minVotingMasterNodes = randomIntBetween(1, allNodes.length + 1);

        final Reconfigurator reconfigurator = makeReconfigurator(
            Settings.builder().put(MINIMUM_VOTING_MASTER_NODES_SETTING.getKey(), minVotingMasterNodes).build());
        final Set<DiscoveryNode> liveNodesSet = nodes(liveNodes);
        final ClusterState.VotingConfiguration initialConfig = conf(initialVotingNodes);
        final ClusterState.VotingConfiguration finalConfig = reconfigurator.reconfigure(liveNodesSet, emptySet(), initialConfig);

        // min configuration size comes from MINIMUM_VOTING_MASTER_NODES_SETTING as long as there are enough nodes in play
        final Set<String> mentionedNodes = Sets.union(Sets.newHashSet(liveNodes), Sets.newHashSet(initialVotingNodes));
        final int minConfigurationSize = Math.min(2 * minVotingMasterNodes - 1, mentionedNodes.size());

        // actual size of a quorum: half the configured nodes, which is all the live nodes plus maybe some dead ones to make up numbers
        final int quorumSize = Math.max(liveNodes.length, minConfigurationSize) / 2 + 1;

        if (quorumSize > liveNodes.length) {
            assertFalse("reconfigure " + liveNodesSet + " from " + initialConfig + " with min of " + minVotingMasterNodes
                + " yielded " + finalConfig + " without a live quorum", finalConfig.hasQuorum(Arrays.asList(liveNodes)));
        } else {
            final List<String> expectedQuorum = randomSubsetOf(quorumSize, liveNodes);
            assertTrue("reconfigure " + liveNodesSet + " from " + initialConfig + " with min of " + minVotingMasterNodes
                    + " yielded " + finalConfig + " with quorum of " + expectedQuorum,
                finalConfig.hasQuorum(expectedQuorum));
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

    private void check(Set<DiscoveryNode> liveNodes, ClusterState.VotingConfiguration config, int minVoterSize,
                       ClusterState.VotingConfiguration expectedConfig) {
        check(liveNodes, retired(), config, minVoterSize, expectedConfig);
    }

    private void check(Set<DiscoveryNode> liveNodes, Set<String> retired, ClusterState.VotingConfiguration config, int minVoterSize,
                       ClusterState.VotingConfiguration expectedConfig) {
        final Reconfigurator reconfigurator = makeReconfigurator(Settings.builder()
            .put(MINIMUM_VOTING_MASTER_NODES_SETTING.getKey(), minVoterSize)
            .build());
        final ClusterState.VotingConfiguration adaptedConfig = reconfigurator.reconfigure(liveNodes, retired, config);
        assertEquals(expectedConfig, adaptedConfig);
    }

    private Reconfigurator makeReconfigurator(Settings settings) {
        return new Reconfigurator(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public void testDynamicSetting() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final Reconfigurator reconfigurator = new Reconfigurator(Settings.EMPTY, clusterSettings);
        final VotingConfiguration initialConfig = conf("a", "b", "c", "d", "e");

        // default min is 1
        assertThat(reconfigurator.reconfigure(nodes("a"), retired(), initialConfig), equalTo(conf("a")));

        // update min to 2
        clusterSettings.applySettings(Settings.builder().put(MINIMUM_VOTING_MASTER_NODES_SETTING.getKey(), 2).build());
        assertThat(reconfigurator.reconfigure(nodes("a"), retired(), initialConfig), sameInstance(initialConfig)); // cannot reconfigure

        expectThrows(IllegalArgumentException.class, () ->
            clusterSettings.applySettings(Settings.builder().put(MINIMUM_VOTING_MASTER_NODES_SETTING.getKey(), 0).build()));
    }
}
