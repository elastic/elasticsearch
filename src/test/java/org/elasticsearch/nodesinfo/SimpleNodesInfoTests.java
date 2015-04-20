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

package org.elasticsearch.nodesinfo;

import com.google.common.collect.Lists;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.nodesinfo.plugin.dummy1.TestPlugin;
import org.elasticsearch.nodesinfo.plugin.dummy2.TestNoVersionPlugin;
import org.elasticsearch.plugins.PluginTestCase;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.nodesInfoRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(scope= Scope.TEST, numDataNodes =0)
public class SimpleNodesInfoTests extends PluginTestCase {

    static final class Fields {
        static final String SITE_PLUGIN = "dummy";
        static final String SITE_PLUGIN_DESCRIPTION = "This is a description for a dummy test site plugin.";
        static final String SITE_PLUGIN_VERSION = "0.0.7-BOND-SITE";
    }


    @Test
    public void testNodesInfos() throws Exception {
        List<String> nodesIds = internalCluster().startNodesAsync(2).get();
        final String node_1 = nodesIds.get(0);
        final String node_2 = nodesIds.get(1);

        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").get();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());

        String server1NodeId = internalCluster().getInstance(ClusterService.class, node_1).state().nodes().localNodeId();
        String server2NodeId = internalCluster().getInstance(ClusterService.class, node_2).state().nodes().localNodeId();
        logger.info("--> started nodes: " + server1NodeId + " and " + server2NodeId);

        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        assertThat(response.getNodes().length, is(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        response = client().admin().cluster().nodesInfo(nodesInfoRequest()).actionGet();
        assertThat(response.getNodes().length, is(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        response = client().admin().cluster().nodesInfo(nodesInfoRequest(server1NodeId)).actionGet();
        assertThat(response.getNodes().length, is(1));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());

        response = client().admin().cluster().nodesInfo(nodesInfoRequest(server1NodeId)).actionGet();
        assertThat(response.getNodes().length, is(1));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());

        response = client().admin().cluster().nodesInfo(nodesInfoRequest(server2NodeId)).actionGet();
        assertThat(response.getNodes().length, is(1));
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        response = client().admin().cluster().nodesInfo(nodesInfoRequest(server2NodeId)).actionGet();
        assertThat(response.getNodes().length, is(1));
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());
    }

    /**
     * Use case is to start 4 nodes:
     * <ul>
     *     <li>1 : no plugin</li>
     *     <li>2 : one site plugin (with a es-plugin.properties file)</li>
     *     <li>3 : one java plugin</li>
     *     <li>4 : one site plugin and 2 java plugins (included the previous one)</li>
     * </ul>
     * We test here that NodeInfo API with plugin option give us the right results.
     * @throws URISyntaxException
     */
    @Test
    public void testNodeInfoPlugin() throws URISyntaxException {
        // We start four nodes
        // The first has no plugin
        String server1NodeId = startNodeWithPlugins(1);
        // The second has one site plugin with a es-plugin.properties file (description and version)
        String server2NodeId = startNodeWithPlugins(2);
        // The third has one java plugin
        String server3NodeId = startNodeWithPlugins(3,TestPlugin.class.getName());
        // The fourth has one java plugin and one site plugin
        String server4NodeId = startNodeWithPlugins(4,TestNoVersionPlugin.class.getName());

        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForNodes("4")).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());

        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().clear().setPlugins(true).execute().actionGet();
        logger.info("--> full json answer, status " + response.toString());

        ElasticsearchAssertions.assertNodeContainsPlugins(response, server1NodeId,
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST, // No JVM Plugin
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST);// No Site Plugin

        ElasticsearchAssertions.assertNodeContainsPlugins(response, server2NodeId,
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST, // No JVM Plugin
                Lists.newArrayList(Fields.SITE_PLUGIN),                                 // Site Plugin
                Lists.newArrayList(Fields.SITE_PLUGIN_DESCRIPTION),
                Lists.newArrayList(Fields.SITE_PLUGIN_VERSION));

        ElasticsearchAssertions.assertNodeContainsPlugins(response, server3NodeId,
                Lists.newArrayList(TestPlugin.Fields.NAME),                             // JVM Plugin
                Lists.newArrayList(TestPlugin.Fields.DESCRIPTION),
                Lists.newArrayList(PluginInfo.VERSION_NOT_AVAILABLE),
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST);// No site Plugin

        ElasticsearchAssertions.assertNodeContainsPlugins(response, server4NodeId,
                Lists.newArrayList(TestNoVersionPlugin.Fields.NAME),                    // JVM Plugin
                Lists.newArrayList(TestNoVersionPlugin.Fields.DESCRIPTION),
                Lists.newArrayList(PluginInfo.VERSION_NOT_AVAILABLE),
                Lists.newArrayList(Fields.SITE_PLUGIN, TestNoVersionPlugin.Fields.NAME),// Site Plugin
                Lists.newArrayList(PluginInfo.DESCRIPTION_NOT_AVAILABLE),
                Lists.newArrayList(PluginInfo.VERSION_NOT_AVAILABLE));
    }

    public String startNodeWithPlugins(int nodeId, String ... pluginClassNames) throws URISyntaxException {
        return startNodeWithPlugins(ImmutableSettings.EMPTY, "/org/elasticsearch/nodesinfo/node" + Integer.toString(nodeId) + "/", pluginClassNames);
    }




}
