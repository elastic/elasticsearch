/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Table;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginRuntimeInfo;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class RestPluginsActionTests extends ESTestCase {
    private final RestPluginsAction action = new RestPluginsAction();

    /**
     * Check that the plugins cat API handles no plugins being installed
     */
    public void testNoPlugins() {
        final Table table = buildTable(List.of());

        assertThat(table.getRows(), is(empty()));
    }

    private Table buildTable(List<PluginDescriptor> pluginDescriptor) {
        final RestRequest request = new FakeRestRequest();

        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < 3; i++) {
            builder.add(node(i));
        }

        final ClusterName clusterName = new ClusterName("test");

        final ClusterState state = ClusterState.builder(clusterName).nodes(builder.build()).build();
        ClusterStateResponse clusterStateResponse = new ClusterStateResponse(clusterName, state, false);

        final List<NodeInfo> nodeInfos = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            nodeInfos.add(
                new NodeInfo(
                    Version.CURRENT,
                        TransportVersion.current(),
                    null,
                    node(i),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    new PluginsAndModules(pluginDescriptor.stream().map(PluginRuntimeInfo::new).toList(), List.of()),
                    null,
                    null,
                    null
                )
            );
        }

        NodesInfoResponse nodesInfoResponse = new NodesInfoResponse(clusterName, nodeInfos, List.of());

        return action.buildTable(request, clusterStateResponse, nodesInfoResponse);
    }

    private DiscoveryNode node(final int id) {
        return DiscoveryNodeUtils.builder(Integer.toString(id)).name("node-" + id).roles(Set.of()).build();
    }
}
