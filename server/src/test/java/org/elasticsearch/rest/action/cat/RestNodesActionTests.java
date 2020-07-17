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

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.Before;

import java.util.Collections;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestNodesActionTests extends ESTestCase {

    private RestNodesAction action;

    @Before
    public void setUpAction() {
        action = new RestNodesAction();
    }

    public void testBuildTableDoesNotThrowGivenNullNodeInfoAndStats() {
        ClusterName clusterName = new ClusterName("cluster-1");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        builder.add(new DiscoveryNode("node-1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));
        DiscoveryNodes discoveryNodes = builder.build();
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        ClusterStateResponse clusterStateResponse = new ClusterStateResponse(clusterName, clusterState, false);
        NodesInfoResponse nodesInfoResponse = new NodesInfoResponse(clusterName, Collections.emptyList(), Collections.emptyList());
        NodesStatsResponse nodesStatsResponse = new NodesStatsResponse(clusterName, Collections.emptyList(), Collections.emptyList());

        action.buildTable(false, new FakeRestRequest(), clusterStateResponse, nodesInfoResponse, nodesStatsResponse);
    }

    public void testCatNodesRejectsLocalParameter() {
        assumeTrue("test is only needed in v8, can be removed in v9", Version.CURRENT.major == Version.V_7_0_0.major + 1);
        TestThreadPool threadPool = new TestThreadPool(RestNodesActionTests.class.getName());
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("local", randomFrom("", "true", "false", randomAlphaOfLength(10)));
        assertThat(expectThrows(IllegalArgumentException.class, () -> action.doCatRequest(request, client)).getMessage(),
            is("parameter [local] is not supported"));
        terminate(threadPool);
    }
}
