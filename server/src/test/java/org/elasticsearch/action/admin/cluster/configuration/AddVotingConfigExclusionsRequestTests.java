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
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfigExclusion;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class AddVotingConfigExclusionsRequestTests extends ESTestCase {
    public void testSerialization() throws IOException {
        int descriptionCount = between(0, 5);
        String[] descriptions = new String[descriptionCount];
        for (int i = 0; i < descriptionCount; i++) {
            descriptions[i] = randomAlphaOfLength(10);
        }
        TimeValue timeout = TimeValue.timeValueMillis(between(0, 30000));
        final AddVotingConfigExclusionsRequest originalRequest = new AddVotingConfigExclusionsRequest(descriptions, timeout);
        final AddVotingConfigExclusionsRequest deserialized = copyWriteable(originalRequest, writableRegistry(),
            AddVotingConfigExclusionsRequest::new);
        assertThat(deserialized.getNodeDescriptions(), equalTo(originalRequest.getNodeDescriptions()));
        assertThat(deserialized.getTimeout(), equalTo(originalRequest.getTimeout()));
    }

    public void testResolve() {
        final DiscoveryNode localNode = new DiscoveryNode(
                "local",
                "local",
                buildNewFakeTransportAddress(),
                emptyMap(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
        final VotingConfigExclusion localNodeExclusion = new VotingConfigExclusion(localNode);
        final DiscoveryNode otherNode1 = new DiscoveryNode(
                "other1",
                "other1",
                buildNewFakeTransportAddress(),
                emptyMap(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
        final VotingConfigExclusion otherNode1Exclusion = new VotingConfigExclusion(otherNode1);
        final DiscoveryNode otherNode2 = new DiscoveryNode(
                "other2",
                "other2",
                buildNewFakeTransportAddress(),
                emptyMap(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
        final VotingConfigExclusion otherNode2Exclusion = new VotingConfigExclusion(otherNode2);
        final DiscoveryNode otherDataNode
            = new DiscoveryNode("data", "data", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster")).nodes(new Builder()
            .add(localNode).add(otherNode1).add(otherNode2).add(otherDataNode).localNodeId(localNode.getId())).build();

        assertThat(makeRequest().resolveVotingConfigExclusions(clusterState),
                containsInAnyOrder(localNodeExclusion, otherNode1Exclusion, otherNode2Exclusion));
        assertThat(makeRequest("_all").resolveVotingConfigExclusions(clusterState),
                containsInAnyOrder(localNodeExclusion, otherNode1Exclusion, otherNode2Exclusion));
        assertThat(makeRequest("_local").resolveVotingConfigExclusions(clusterState),
                contains(localNodeExclusion));
        assertThat(makeRequest("other*").resolveVotingConfigExclusions(clusterState),
                containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion));

        assertThat(expectThrows(IllegalArgumentException.class,
                () -> makeRequest("not-a-node").resolveVotingConfigExclusions(clusterState)).getMessage(),
                    equalTo("add voting config exclusions request for [not-a-node] matched no master-eligible nodes"));
    }

    public void testResolveAndCheckMaximum() {
        final DiscoveryNode localNode = new DiscoveryNode(
                "local",
                "local",
                buildNewFakeTransportAddress(),
                emptyMap(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
        final VotingConfigExclusion localNodeExclusion = new VotingConfigExclusion(localNode);
        final DiscoveryNode otherNode1 = new DiscoveryNode(
                "other1",
                "other1",
                buildNewFakeTransportAddress(),
                emptyMap(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
        final VotingConfigExclusion otherNode1Exclusion = new VotingConfigExclusion(otherNode1);
        final DiscoveryNode otherNode2 = new DiscoveryNode(
                "other2",
                "other2",
                buildNewFakeTransportAddress(),
                emptyMap(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
        final VotingConfigExclusion otherNode2Exclusion = new VotingConfigExclusion(otherNode2);

        final ClusterState.Builder builder = ClusterState.builder(new ClusterName("cluster")).nodes(new Builder()
            .add(localNode).add(otherNode1).add(otherNode2).localNodeId(localNode.getId()));
        builder.metaData(MetaData.builder()
                .coordinationMetaData(CoordinationMetaData.builder().addVotingConfigExclusion(otherNode1Exclusion).build()));
        final ClusterState clusterState = builder.build();

        assertThat(makeRequest().resolveVotingConfigExclusionsAndCheckMaximum(clusterState, 3, "setting.name"),
                containsInAnyOrder(localNodeExclusion, otherNode2Exclusion));
        assertThat(makeRequest("_local").resolveVotingConfigExclusionsAndCheckMaximum(clusterState, 2, "setting.name"),
                contains(localNodeExclusion));

        assertThat(expectThrows(IllegalArgumentException.class,
            () -> makeRequest().resolveVotingConfigExclusionsAndCheckMaximum(clusterState, 2, "setting.name")).getMessage(),
            equalTo("add voting config exclusions request for [] would add [2] exclusions to the existing [1] which would exceed " +
                "the maximum of [2] set by [setting.name]"));
        assertThat(expectThrows(IllegalArgumentException.class,
            () -> makeRequest("_local").resolveVotingConfigExclusionsAndCheckMaximum(clusterState, 1, "setting.name")).getMessage(),
            equalTo("add voting config exclusions request for [_local] would add [1] exclusions to the existing [1] which would " +
                "exceed the maximum of [1] set by [setting.name]"));
    }

    private static AddVotingConfigExclusionsRequest makeRequest(String... descriptions) {
        return new AddVotingConfigExclusionsRequest(descriptions);
    }
}
