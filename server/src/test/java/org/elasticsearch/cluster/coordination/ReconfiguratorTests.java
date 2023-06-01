/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.coordination.Reconfigurator.CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION;
import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ReconfiguratorTests extends ESTestCase {

    @Before
    public void resetPortCounterBeforeTest() {
        resetPortCounter();
    }

    public void testReconfigurationExamples() {

        check(nodes("a"), conf("a"), true, conf("a"));
        check(nodes("a", "b"), conf("a"), true, conf("a"));
        check(nodes("a", "b"), conf("b"), true, conf("a"));
        check(nodes("a", "b"), conf("a", "c"), true, conf("a"));
        check(nodes("a", "b"), conf("a", "b"), true, conf("a"));
        check(nodes("a", "b"), conf("a", "b", "e"), true, conf("a", "b", "e"));
        check(nodes("a", "b", "c"), conf("a"), true, conf("a", "b", "c"));
        check(nodes("a", "b", "c"), conf("a", "b"), true, conf("a", "b", "c"));
        check(nodes("a", "b", "c"), conf("a", "b", "c"), true, conf("a", "b", "c"));
        check(nodes("a", "b", "c", "d"), conf("a", "b", "c"), true, conf("a", "b", "c"));
        check(nodes("a", "b", "c", "d", "e"), conf("a", "b", "c"), true, conf("a", "b", "c", "d", "e"));
        check(nodes("a", "b", "c"), conf("a", "b", "e"), true, conf("a", "b", "c"));
        check(nodes("a", "b", "c", "d"), conf("a", "b", "e"), true, conf("a", "b", "c"));
        check(nodes("a", "b", "c", "d", "e"), conf("a", "f", "g"), true, conf("a", "b", "c", "d", "e"));
        check(nodes("a", "b", "c", "d"), conf("a", "b", "c", "d", "e"), true, conf("a", "b", "c"));
        check(nodes("e", "a", "b", "c"), retired(), "e", conf("a", "b", "c", "d", "e"), true, conf("a", "b", "e"));
        check(nodes("a", "b", "c"), conf("a", "b", "c", "d", "e"), true, conf("a", "b", "c"));

        check(nodes("a"), conf("a"), false, conf("a"));
        check(nodes("a", "b"), conf("a"), false, conf("a"));
        check(nodes("a", "b"), conf("a", "b"), false, conf("a", "b"));
        check(nodes("a", "b", "c"), conf("a"), false, conf("a", "b", "c"));
        check(nodes("a", "b", "c"), conf("a", "b"), false, conf("a", "b", "c"));
        check(nodes("a", "b"), conf("a", "b", "e"), false, conf("a", "b", "e"));
        check(nodes("a", "b"), conf("a", "c"), false, conf("a", "b"));
        check(nodes("a", "b"), conf("a", "b", "e"), false, conf("a", "b", "e"));
        check(nodes("a", "b", "c"), conf("a", "b", "c"), false, conf("a", "b", "c"));
        check(nodes("a", "b", "c", "d"), conf("a", "b", "c"), false, conf("a", "b", "c"));
        check(nodes("a", "b", "c", "d", "e"), conf("a", "b", "c"), false, conf("a", "b", "c", "d", "e"));
        check(nodes("a", "b", "c"), conf("a", "b", "e"), false, conf("a", "b", "c"));
        check(nodes("a", "b", "c", "d"), conf("a", "b", "e"), false, conf("a", "b", "c"));
        check(nodes("a", "b", "c", "d", "e"), conf("a", "f", "g"), false, conf("a", "b", "c", "d", "e"));
        check(nodes("a", "b", "c", "d"), conf("a", "b", "c", "d", "e"), false, conf("a", "b", "c", "d", "e"));
        check(nodes("a", "b", "c"), conf("a", "b", "c", "d", "e"), false, conf("a", "b", "c", "d", "e"));

        // Retiring a single node shifts the votes elsewhere if possible.
        check(nodes("a", "b"), retired("a"), conf("a"), true, conf("b"));
        check(nodes("a", "b"), retired("a"), conf("a"), false, conf("b"));

        // Retiring a node from a three-node cluster drops down to a one-node configuration
        check(nodes("a", "b", "c"), retired("a"), conf("a"), true, conf("b"));
        check(nodes("a", "b", "c"), retired("a"), conf("a", "b", "c"), true, conf("b"));
        check(nodes("a", "b", "c"), retired("a"), conf("a"), false, conf("b"));
        check(nodes("a", "b", "c"), retired("a"), conf("a", "b", "c"), false, conf("b", "c"));

        // 7 nodes, one for each combination of live/retired/current. Ideally we want the config to be the non-retired live nodes.
        // Since there are 2 non-retired live nodes we round down to 1 and just use the one that's already in the config.
        check(nodes("a", "b", "c", "f"), retired("c", "e", "f", "g"), conf("a", "c", "d", "e"), true, conf("a"));
        // Only two non-retired nodes in the config, so new config does not shrink below 2
        check(nodes("a", "b", "c", "f"), retired("c", "e", "f", "g"), conf("a", "c", "d", "e"), false, conf("a", "b"));

        // The config has at least three non-retired nodes so does not shrink below 3
        check(nodes("a", "b", "c", "f"), retired("c", "e", "f", "g"), conf("a", "c", "d", "e", "h"), true, conf("a", "b", "d"));
        // Three non-retired nodes in the config, so new config does not shrink below 3
        check(nodes("a", "b", "c", "f"), retired("c", "e", "f", "g"), conf("a", "c", "d", "e", "h"), false, conf("a", "b", "d"));
    }

    public void testAutoShrinking() {
        final String[] allNodes = new String[] { "a", "b", "c", "d", "e", "f", "g" };

        final String[] liveNodes = new String[randomIntBetween(1, allNodes.length)];
        randomSubsetOf(liveNodes.length, allNodes).toArray(liveNodes);

        final String[] initialVotingNodes = new String[randomIntBetween(1, allNodes.length)];
        randomSubsetOf(initialVotingNodes.length, allNodes).toArray(initialVotingNodes);

        final Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            settingsBuilder.put(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.getKey(), true);
        }
        final Reconfigurator reconfigurator = makeReconfigurator(settingsBuilder.build());
        final Set<DiscoveryNode> liveNodesSet = nodes(liveNodes);
        final VotingConfiguration initialConfig = conf(initialVotingNodes);

        final int quorumSize = Math.max(liveNodes.length / 2 + 1, initialVotingNodes.length < 3 ? 1 : 2);

        final VotingConfiguration finalConfig = reconfigurator.reconfigure(
            liveNodesSet,
            emptySet(),
            randomFrom(liveNodesSet),
            initialConfig
        );

        final String description = "reconfigure " + liveNodesSet + " from " + initialConfig + " yielded " + finalConfig;

        if (quorumSize > liveNodes.length) {
            assertFalse(description + " without a live quorum", finalConfig.hasQuorum(Arrays.asList(liveNodes)));
        } else {
            final List<String> expectedQuorum = randomSubsetOf(quorumSize, liveNodes);
            assertTrue(description + " with quorum[" + quorumSize + "] of " + expectedQuorum, finalConfig.hasQuorum(expectedQuorum));
        }
    }

    public void testManualShrinking() {
        final String[] allNodes = new String[] { "a", "b", "c", "d", "e", "f", "g" };

        final String[] liveNodes = new String[randomIntBetween(1, allNodes.length)];
        randomSubsetOf(liveNodes.length, allNodes).toArray(liveNodes);

        final String[] initialVotingNodes = new String[randomIntBetween(1, allNodes.length)];
        randomSubsetOf(initialVotingNodes.length, allNodes).toArray(initialVotingNodes);

        final Reconfigurator reconfigurator = makeReconfigurator(
            Settings.builder().put(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.getKey(), false).build()
        );
        final Set<DiscoveryNode> liveNodesSet = nodes(liveNodes);
        final VotingConfiguration initialConfig = conf(initialVotingNodes);

        final int quorumSize = Math.max(liveNodes.length, initialVotingNodes.length) / 2 + 1;

        final VotingConfiguration finalConfig = reconfigurator.reconfigure(
            liveNodesSet,
            emptySet(),
            randomFrom(liveNodesSet),
            initialConfig
        );

        final String description = "reconfigure " + liveNodesSet + " from " + initialConfig + " yielded " + finalConfig;

        if (quorumSize > liveNodes.length) {
            assertFalse(description + " without a live quorum", finalConfig.hasQuorum(Arrays.asList(liveNodes)));
        } else {
            final List<String> expectedQuorum = randomSubsetOf(quorumSize, liveNodes);
            assertTrue(description + " with quorum[" + quorumSize + "] of " + expectedQuorum, finalConfig.hasQuorum(expectedQuorum));
        }
    }

    private VotingConfiguration conf(String... nodes) {
        return new VotingConfiguration(Sets.newHashSet(nodes));
    }

    private Set<DiscoveryNode> nodes(String... nodes) {
        final Set<DiscoveryNode> liveNodes = new HashSet<>();
        for (String id : nodes) {
            liveNodes.add(DiscoveryNodeUtils.create(id));
        }
        return liveNodes;
    }

    private Set<String> retired(String... nodes) {
        return Arrays.stream(nodes).collect(Collectors.toSet());
    }

    private void check(
        Set<DiscoveryNode> liveNodes,
        VotingConfiguration config,
        boolean autoShrinkVotingConfiguration,
        VotingConfiguration expectedConfig
    ) {
        check(liveNodes, retired(), config, autoShrinkVotingConfiguration, expectedConfig);
    }

    private void check(
        Set<DiscoveryNode> liveNodes,
        Set<String> retired,
        VotingConfiguration config,
        boolean autoShrinkVotingConfiguration,
        VotingConfiguration expectedConfig
    ) {
        final DiscoveryNode master = liveNodes.stream().sorted(Comparator.comparing(DiscoveryNode::getId)).findFirst().get();
        check(liveNodes, retired, master.getId(), config, autoShrinkVotingConfiguration, expectedConfig);
    }

    private void check(
        Set<DiscoveryNode> liveNodes,
        Set<String> retired,
        String masterId,
        VotingConfiguration config,
        boolean autoShrinkVotingConfiguration,
        VotingConfiguration expectedConfig
    ) {
        final Reconfigurator reconfigurator = makeReconfigurator(
            Settings.builder().put(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.getKey(), autoShrinkVotingConfiguration).build()
        );

        final DiscoveryNode master = liveNodes.stream().filter(n -> n.getId().equals(masterId)).findFirst().get();
        final VotingConfiguration adaptedConfig = reconfigurator.reconfigure(liveNodes, retired, master, config);
        assertEquals(
            format(
                "[liveNodes=%s, retired=%s, master=%s, config=%s, autoShrinkVotingConfiguration=%s]",
                liveNodes,
                retired,
                master,
                config,
                autoShrinkVotingConfiguration
            ),
            expectedConfig,
            adaptedConfig
        );
    }

    private Reconfigurator makeReconfigurator(Settings settings) {
        return new Reconfigurator(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public void testDynamicSetting() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final Reconfigurator reconfigurator = new Reconfigurator(Settings.EMPTY, clusterSettings);
        final VotingConfiguration initialConfig = conf("a", "b", "c", "d", "e");

        Set<DiscoveryNode> twoNodes = nodes("a", "b");
        Set<DiscoveryNode> threeNodes = nodes("a", "b", "c");

        // default is "true"
        assertThat(reconfigurator.reconfigure(twoNodes, retired(), randomFrom(twoNodes), initialConfig), equalTo(conf("a", "b", "c")));

        // update to "false"
        clusterSettings.applySettings(Settings.builder().put(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.getKey(), "false").build());
        // no quorum
        assertThat(reconfigurator.reconfigure(twoNodes, retired(), randomFrom(twoNodes), initialConfig), sameInstance(initialConfig));
        assertThat(
            reconfigurator.reconfigure(threeNodes, retired(), randomFrom(threeNodes), initialConfig),
            equalTo(conf("a", "b", "c", "d", "e"))
        );
        assertThat(
            reconfigurator.reconfigure(threeNodes, retired("d"), randomFrom(threeNodes), initialConfig),
            equalTo(conf("a", "b", "c", "e"))
        );

        // explicitly set to "true"
        clusterSettings.applySettings(Settings.builder().put(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.getKey(), "true").build());
        assertThat(reconfigurator.reconfigure(twoNodes, retired(), randomFrom(twoNodes), initialConfig), equalTo(conf("a", "b", "c")));

        expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.getKey(), "blah").build())
        );
    }
}
