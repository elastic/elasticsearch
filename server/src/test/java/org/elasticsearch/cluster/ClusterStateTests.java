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
package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class ClusterStateTests extends ESTestCase {

    public void testSupersedes() {
        final Version version = Version.CURRENT;
        final DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), version);
        final DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), version);
        final DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).add(node2).build();
        ClusterName name = ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY);
        ClusterState noMaster1 = ClusterState.builder(name).version(randomInt(5)).nodes(nodes).build();
        ClusterState noMaster2 = ClusterState.builder(name).version(randomInt(5)).nodes(nodes).build();
        ClusterState withMaster1a = ClusterState.builder(name).version(randomInt(5)).nodes(DiscoveryNodes.builder(nodes)
            .masterNodeId(node1.getId())).build();
        ClusterState withMaster1b = ClusterState.builder(name).version(randomInt(5)).nodes(DiscoveryNodes.builder(nodes)
            .masterNodeId(node1.getId())).build();
        ClusterState withMaster2 = ClusterState.builder(name).version(randomInt(5)).nodes(DiscoveryNodes.builder(nodes)
            .masterNodeId(node2.getId())).build();

        // states with no master should never supersede anything
        assertFalse(noMaster1.supersedes(noMaster2));
        assertFalse(noMaster1.supersedes(withMaster1a));

        // states should never supersede states from another master
        assertFalse(withMaster1a.supersedes(withMaster2));
        assertFalse(withMaster1a.supersedes(noMaster1));

        // state from the same master compare by version
        assertThat(withMaster1a.supersedes(withMaster1b), equalTo(withMaster1a.version() > withMaster1b.version()));
    }

    public void testVotingConfiguration() {
        VotingConfiguration config0 = new VotingConfiguration(Sets.newHashSet());
        assertThat(config0, equalTo(VotingConfiguration.EMPTY_CONFIG));
        assertThat(config0.getNodeIds(), equalTo(Sets.newHashSet()));
        assertThat(config0.isEmpty(), equalTo(true));
        assertThat(config0.hasQuorum(Sets.newHashSet()), equalTo(false));
        assertThat(config0.hasQuorum(Sets.newHashSet("id1")), equalTo(false));

        VotingConfiguration config1 = new VotingConfiguration(Sets.newHashSet("id1"));
        assertThat(config1.getNodeIds(), equalTo(Sets.newHashSet("id1")));
        assertThat(config1.isEmpty(), equalTo(false));
        assertThat(config1.hasQuorum(Sets.newHashSet("id1")), equalTo(true));
        assertThat(config1.hasQuorum(Sets.newHashSet("id1", "id2")), equalTo(true));
        assertThat(config1.hasQuorum(Sets.newHashSet("id2")), equalTo(false));
        assertThat(config1.hasQuorum(Sets.newHashSet()), equalTo(false));

        VotingConfiguration config2 = new VotingConfiguration(Sets.newHashSet("id1", "id2"));
        assertThat(config2.getNodeIds(), equalTo(Sets.newHashSet("id1", "id2")));
        assertThat(config2.isEmpty(), equalTo(false));
        assertThat(config2.hasQuorum(Sets.newHashSet("id1", "id2")), equalTo(true));
        assertThat(config2.hasQuorum(Sets.newHashSet("id1", "id2", "id3")), equalTo(true));
        assertThat(config2.hasQuorum(Sets.newHashSet("id1")), equalTo(false));
        assertThat(config2.hasQuorum(Sets.newHashSet("id2")), equalTo(false));
        assertThat(config2.hasQuorum(Sets.newHashSet("id3")), equalTo(false));
        assertThat(config2.hasQuorum(Sets.newHashSet("id1", "id3")), equalTo(false));
        assertThat(config2.hasQuorum(Sets.newHashSet()), equalTo(false));

        VotingConfiguration config3 = new VotingConfiguration(Sets.newHashSet("id1", "id2", "id3"));
        assertThat(config3.getNodeIds(), equalTo(Sets.newHashSet("id1", "id2", "id3")));
        assertThat(config3.isEmpty(), equalTo(false));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1", "id2")), equalTo(true));
        assertThat(config3.hasQuorum(Sets.newHashSet("id2", "id3")), equalTo(true));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1", "id3")), equalTo(true));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1", "id2", "id3")), equalTo(true));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1", "id2", "id4")), equalTo(true));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1")), equalTo(false));
        assertThat(config3.hasQuorum(Sets.newHashSet("id2")), equalTo(false));
        assertThat(config3.hasQuorum(Sets.newHashSet("id3")), equalTo(false));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1", "id4")), equalTo(false));
        assertThat(config3.hasQuorum(Sets.newHashSet("id1", "id4", "id5")), equalTo(false));
        assertThat(config3.hasQuorum(Sets.newHashSet()), equalTo(false));
    }

    public void testVotingConfigurationSerializationEqualsHashCode() {
        VotingConfiguration initialConfig = new VotingConfiguration(
            Sets.newHashSet(generateRandomStringArray(randomInt(10), 20, false)));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialConfig,
            orig -> ESTestCase.copyWriteable(orig, new NamedWriteableRegistry(Collections.emptyList()), VotingConfiguration::new),
            cfg -> {
                Set<String> newNodeIds = new HashSet<>(cfg.getNodeIds());
                if (cfg.isEmpty() == false && randomBoolean()) {
                    // remove random element
                    newNodeIds.remove(randomFrom(cfg.getNodeIds()));
                } else if (cfg.isEmpty() == false && randomBoolean()) {
                    // change random element
                    newNodeIds.remove(randomFrom(cfg.getNodeIds()));
                    newNodeIds.add(randomAlphaOfLength(20));
                } else {
                    // add random element
                    newNodeIds.add(randomAlphaOfLength(20));
                }
                return new VotingConfiguration(newNodeIds);
            });
    }
}
