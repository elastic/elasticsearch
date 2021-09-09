/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Table;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.plugins.PluginType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class RestPluginsActionTests extends ESTestCase {
    private final RestPluginsAction action = new RestPluginsAction();

    /**
     * Check that the plugins cat API handles no plugins being installed
     */
    public void testNoPlugins() {
        final Table table = buildTable(List.of(), false);

        assertThat(table.getRows(), is(empty()));
    }

    /**
     * Check that the plugins cat API excludes bootstrap plugins when they are not requested.
     */
    public void testIsolatedPluginOnly() {
        final Table table = buildTable(
            List.of(
                plugin("test-plugin", PluginType.ISOLATED),
                plugin("ignored-plugin", PluginType.BOOTSTRAP)
            ),
            false
        );

        // verify the table headers are correct
        final List<Object> headers = table.getHeaders().stream().map(h -> h.value).collect(Collectors.toList());
        assertThat(headers, contains("id", "name", "component", "version", "description", "type"));

        // verify the table rows are correct
        final List<List<String>> rows = table.getRows()
            .stream()
            .map(row -> row.stream().map(c -> String.valueOf(c.value)).collect(Collectors.toList()))
            .collect(Collectors.toList());
        assertThat(rows, hasSize(3));

        final List<Matcher<? super List<String>>> matchers = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            matchers.add(contains(Integer.toString(i), "node-" + i, "test-plugin", "1.0", "test-plugin description", "isolated"));
        }

        assertThat(rows, containsInAnyOrder(matchers));
    }

    /**
     * Check that the plugins cat API includes bootstrap plugins when they are requested.
     */
    public void testIncludeBootstrap() {
        final Table table = buildTable(
            List.of(plugin("test-plugin", PluginType.ISOLATED), plugin("bootstrap-plugin", PluginType.BOOTSTRAP)),
            true
        );

        // verify the table rows are correct
        final List<List<String>> rows = table.getRows()
            .stream()
            .map(row -> row.stream().map(c -> String.valueOf(c.value)).collect(Collectors.toList()))
            .collect(Collectors.toList());
        assertThat(rows, hasSize(6));

        final List<Matcher<? super List<String>>> matchers = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            for (String pluginName : List.of("test-plugin", "bootstrap-plugin")) {
                matchers.add(
                    contains(
                        Integer.toString(i),
                        "node-" + i,
                        pluginName,
                        "1.0",
                        pluginName + " description",
                        pluginName.contains("bootstrap") ? "bootstrap" : "isolated"
                    )
                );
            }
        }

        assertThat(rows, containsInAnyOrder(matchers));
    }

    private Table buildTable(List<PluginInfo> pluginInfo, boolean includeBootstrap) {
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
                    null,
                    node(i),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    new PluginsAndModules(pluginInfo, List.of()),
                    null,
                    null,
                    null
                )
            );
        }

        NodesInfoResponse nodesInfoResponse = new NodesInfoResponse(clusterName, nodeInfos, List.of());

        return action.buildTable(request, clusterStateResponse, nodesInfoResponse, includeBootstrap);
    }

    private DiscoveryNode node(final int id) {
        return new DiscoveryNode("node-" + id, Integer.toString(id), buildNewFakeTransportAddress(), Map.of(), Set.of(), Version.CURRENT);
    }

    private PluginInfo plugin(String name, PluginType type) {
        return new PluginInfo(name, name + " description", "1.0", null, null, null, List.of(), false, type, null, false);
    }
}
