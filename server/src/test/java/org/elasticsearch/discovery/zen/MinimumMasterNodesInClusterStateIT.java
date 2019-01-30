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
package org.elasticsearch.discovery.zen;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.discovery.zen.ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING;
import static org.elasticsearch.test.InternalTestCluster.nameFilter;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isIn;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class MinimumMasterNodesInClusterStateIT extends ESIntegTestCase {

    public void testMasterPublishes() throws Exception {
        final String firstNode = internalCluster().startNode();

        {
            final ClusterState localState
                = client(firstNode).admin().cluster().state(new ClusterStateRequest().local(true)).get().getState();
            assertThat(localState.getMinimumMasterNodesOnPublishingMaster(), equalTo(1));
            assertFalse(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.exists(localState.metaData().settings()));
        }

        final List<String> secondThirdNodes = internalCluster().startNodes(2);
        assertThat(internalCluster().getMasterName(), equalTo(firstNode));

        final List<String> allNodes = Stream.concat(Stream.of(firstNode), secondThirdNodes.stream()).collect(Collectors.toList());
        for (final String node : allNodes) {
            final ClusterState localState = client(node).admin().cluster().state(new ClusterStateRequest().local(true)).get().getState();
            assertThat(localState.getMinimumMasterNodesOnPublishingMaster(), equalTo(1));
            assertThat(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.get(localState.metaData().settings()), equalTo(2));
        }

        internalCluster().stopRandomNode(nameFilter(firstNode));
        assertThat(internalCluster().getMasterName(), isIn(secondThirdNodes));

        for (final String node : secondThirdNodes) {
            final ClusterState localState = client(node).admin().cluster().state(new ClusterStateRequest().local(true)).get().getState();
            assertThat(localState.getMinimumMasterNodesOnPublishingMaster(), equalTo(2));
            assertThat(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.get(localState.metaData().settings()), equalTo(2));
        }
    }
}
