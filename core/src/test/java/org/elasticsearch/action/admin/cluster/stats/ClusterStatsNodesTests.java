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

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;

public class ClusterStatsNodesTests extends ESTestCase {

    /**
     * Test that empty transport/http types are not printed out as part
     * of the cluster stats xcontent output.
     */
    public void testNetworkTypesToXContent() throws Exception {
        ClusterStatsNodes.NetworkTypes stats = new ClusterStatsNodes.NetworkTypes(emptyList());
        assertEquals("{\"transport_types\":{},\"http_types\":{}}",
                toXContent(stats, XContentType.JSON, randomBoolean()).utf8ToString());

        List<NodeInfo> nodeInfos = singletonList(createNodeInfo("node_0", null, null));
        stats = new ClusterStatsNodes.NetworkTypes(nodeInfos);
        assertEquals("{\"transport_types\":{},\"http_types\":{}}",
                toXContent(stats, XContentType.JSON, randomBoolean()).utf8ToString());

        nodeInfos = Arrays.asList(createNodeInfo("node_1", "", ""),
                                  createNodeInfo("node_2", "custom", "custom"),
                                  createNodeInfo("node_3", null, "custom"));
        stats = new ClusterStatsNodes.NetworkTypes(nodeInfos);
        assertEquals("{"
                + "\"transport_types\":{\"custom\":1},"
                + "\"http_types\":{\"custom\":2}"
        + "}", toXContent(stats, XContentType.JSON, randomBoolean()).utf8ToString());
    }

    private static NodeInfo createNodeInfo(String nodeId, String transportType, String httpType) {
        Settings.Builder settings = Settings.builder();
        if (transportType != null) {
            settings.put(randomFrom(NetworkModule.TRANSPORT_TYPE_KEY,
                    NetworkModule.TRANSPORT_TYPE_DEFAULT_KEY), transportType);
        }
        if (httpType != null) {
            settings.put(randomFrom(NetworkModule.HTTP_TYPE_KEY,
                    NetworkModule.HTTP_TYPE_DEFAULT_KEY), httpType);
        }
        return new NodeInfo(null, null,
                new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), null),
                settings.build(), null, null, null, null, null, null, null, null, null);
    }
}
