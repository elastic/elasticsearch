/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.nodesinfo;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsInfo;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.elasticsearch.test.integration.nodesinfo.plugin.dummy1.TestPlugin;
import org.elasticsearch.test.integration.nodesinfo.plugin.dummy2.TestNoVersionPlugin;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.nodesInfoRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleNodesInfoTests extends AbstractNodesTests {

    static final class Fields {
        static final String SITE_PLUGIN = "dummy";
        static final String SITE_PLUGIN_DESCRIPTION = "This is a description for a dummy test site plugin.";
        static final String SITE_PLUGIN_NO_DESCRIPTION = "No description found for dummy.";
        static final String JVM_PLUGIN_NO_DESCRIPTION = "No description found for test-no-version-plugin.";
    }

    @AfterMethod
    public void closeNodes() {
        closeAllNodes();
    }

    @Test
    public void testNodesInfos() {
        startNode("server1");
        startNode("server2");

        ClusterHealthResponse clusterHealth = client("server2").admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());

        String server1NodeId = ((InternalNode) node("server1")).injector().getInstance(ClusterService.class).state().nodes().localNodeId();
        String server2NodeId = ((InternalNode) node("server2")).injector().getInstance(ClusterService.class).state().nodes().localNodeId();
        logger.info("--> started nodes: " + server1NodeId + " and " + server2NodeId);

        NodesInfoResponse response = client("server1").admin().cluster().prepareNodesInfo().execute().actionGet();
        assertThat(response.getNodes().length, equalTo(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        response = client("server2").admin().cluster().nodesInfo(nodesInfoRequest()).actionGet();
        assertThat(response.getNodes().length, equalTo(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        response = client("server1").admin().cluster().nodesInfo(nodesInfoRequest(server1NodeId)).actionGet();
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());

        response = client("server2").admin().cluster().nodesInfo(nodesInfoRequest(server1NodeId)).actionGet();
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());

        response = client("server1").admin().cluster().nodesInfo(nodesInfoRequest(server2NodeId)).actionGet();
        assertThat(response.getNodes().length, equalTo(1));
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        response = client("server2").admin().cluster().nodesInfo(nodesInfoRequest(server2NodeId)).actionGet();
        assertThat(response.getNodes().length, equalTo(1));
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
        String server1NodeId = startNodeWithPlugins("node1");
        // The second has one site plugin with a es-plugin.properties file (description and version)
        String server2NodeId = startNodeWithPlugins("node2");
        // The third has one java plugin
        String server3NodeId = startNodeWithPlugins("node3");
        // The fourth has one java plugin and one site plugin
        String server4NodeId = startNodeWithPlugins("node4");

        ClusterHealthResponse clusterHealth = client("node4").admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());

        NodesInfoResponse response = client("node1").admin().cluster().prepareNodesInfo().setPlugin(true).execute().actionGet();
        logger.info("--> full json answer, status " + response.toString());

        checkPlugin(response, server1NodeId, 0, 0);
        checkPlugin(response, server2NodeId, 1, 0);
        checkPlugin(response, server3NodeId, 0, 1);
        checkPlugin(response, server4NodeId, 1, 2); // Note that we have now 2 JVM plugins as we have already loaded one with node3
    }

    /**
     * We check infos
     * @param response Response
     * @param nodeId NodeId we want to check
     * @param expectedSitePlugins Number of site plugins expected
     * @param expectedJvmPlugins Number of jvm plugins expected
     */
    private void checkPlugin(NodesInfoResponse response, String nodeId,
                             int expectedSitePlugins, int expectedJvmPlugins) {
        assertThat(response.getNodesMap().get(nodeId), notNullValue());

        PluginsInfo plugins = response.getNodesMap().get(nodeId).getPlugins();
        assertThat(plugins, notNullValue());

        int num_site_plugins = 0;
        int num_jvm_plugins = 0;

        for (PluginInfo pluginInfo : plugins.getInfos()) {
            // It should be a site or a jvm plugin
            assertThat(pluginInfo.isJvm() || pluginInfo.isSite(), is(true));

            if (pluginInfo.isSite() && !pluginInfo.isJvm()) {
                // Let's do some tests for site plugins
                assertThat(pluginInfo.getName(), isOneOf(Fields.SITE_PLUGIN,
                        TestNoVersionPlugin.Fields.NAME));
                assertThat(pluginInfo.getDescription(),
                        isOneOf(Fields.SITE_PLUGIN_DESCRIPTION,
                                Fields.SITE_PLUGIN_NO_DESCRIPTION,
                                Fields.JVM_PLUGIN_NO_DESCRIPTION));
                assertThat(pluginInfo.getUrl(), notNullValue());
                num_site_plugins++;
            }

            if (pluginInfo.isJvm() && !pluginInfo.isSite()) {
                // Let's do some tests for site plugins
                assertThat(pluginInfo.getName(),
                        isOneOf(TestPlugin.Fields.NAME, TestNoVersionPlugin.Fields.NAME));
                assertThat(pluginInfo.getDescription(),
                        isOneOf(TestPlugin.Fields.DESCRIPTION, TestNoVersionPlugin.Fields.DESCRIPTION));
                assertThat(pluginInfo.getUrl(), nullValue());
                num_jvm_plugins++;
            }

            // On node4, test-no-version-plugin has an embedded _site structure
            if (pluginInfo.isJvm() && pluginInfo.isSite()) {
                assertThat(pluginInfo.getName(),
                        is(TestNoVersionPlugin.Fields.NAME));
                assertThat(pluginInfo.getDescription(),
                        is(TestNoVersionPlugin.Fields.DESCRIPTION));
                assertThat(pluginInfo.getUrl(), notNullValue());
                num_jvm_plugins++;
            }
        }

        assertThat(num_site_plugins, is(expectedSitePlugins));
        assertThat(num_jvm_plugins, is(expectedJvmPlugins));
    }

    private String startNodeWithPlugins(String name) throws URISyntaxException {
        URL resource = SimpleNodesInfoTests.class.getResource("/org/elasticsearch/test/integration/nodesinfo/" + name + "/");
        ImmutableSettings.Builder settings = settingsBuilder();
        if (resource != null) {
            settings.put("path.plugins", new File(resource.toURI()).getAbsolutePath());
        }

        startNode(name, settings);

        // We wait for a Green status
        client(name).admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();

        String serverNodeId = ((InternalNode) node(name)).injector()
                .getInstance(ClusterService.class).state().nodes().localNodeId();
        logger.debug("--> server {} started" + serverNodeId);
        return serverNodeId;
    }

}
