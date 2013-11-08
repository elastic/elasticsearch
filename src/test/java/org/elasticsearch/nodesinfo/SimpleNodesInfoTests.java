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

package org.elasticsearch.nodesinfo;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsInfo;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.nodesinfo.plugin.dummy1.TestPlugin;
import org.elasticsearch.nodesinfo.plugin.dummy2.TestNoVersionPlugin;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.isNull;
import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.nodesInfoRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(scope=Scope.TEST, numNodes=0)
public class SimpleNodesInfoTests extends ElasticsearchIntegrationTest {

    static final class Fields {
        static final String SITE_PLUGIN = "dummy";
        static final String SITE_PLUGIN_DESCRIPTION = "This is a description for a dummy test site plugin.";
        static final String SITE_PLUGIN_NO_DESCRIPTION = "No description found for dummy.";
    }


    @Test
    public void testNodesInfos() {
        final String node_1 = cluster().startNode();
        final String node_2 = cluster().startNode();

        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());

        String server1NodeId = cluster().getInstance(ClusterService.class, node_1).state().nodes().localNodeId();
        String server2NodeId = cluster().getInstance(ClusterService.class, node_2).state().nodes().localNodeId();
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

        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());

        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().setPlugin(true).execute().actionGet();
        logger.info("--> full json answer, status " + response.toString());

        assertNodeContainsPlugins(response, server1NodeId, Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        assertNodeContainsPlugins(response, server2NodeId, Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                Lists.newArrayList(Fields.SITE_PLUGIN),
                Lists.newArrayList(Fields.SITE_PLUGIN_DESCRIPTION));

        assertNodeContainsPlugins(response, server3NodeId, Lists.newArrayList(TestPlugin.Fields.NAME),
                Lists.newArrayList(TestPlugin.Fields.DESCRIPTION),
                Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        assertNodeContainsPlugins(response, server4NodeId,
                Lists.newArrayList(TestNoVersionPlugin.Fields.NAME),
                Lists.newArrayList(TestNoVersionPlugin.Fields.DESCRIPTION),
                Lists.newArrayList(Fields.SITE_PLUGIN, TestNoVersionPlugin.Fields.NAME),
                Lists.newArrayList(Fields.SITE_PLUGIN_NO_DESCRIPTION, TestNoVersionPlugin.Fields.DESCRIPTION));
    }

    private void assertNodeContainsPlugins(NodesInfoResponse response, String nodeId,
                                           List<String> expectedJvmPluginNames,
                                           List<String> expectedJvmPluginDescriptions,
                                           List<String> expectedSitePluginNames,
                                           List<String> expectedSitePluginDescriptions) {

        assertThat(response.getNodesMap().get(nodeId), notNullValue());

        PluginsInfo plugins = response.getNodesMap().get(nodeId).getPlugins();
        assertThat(plugins, notNullValue());

        List<String> pluginNames = FluentIterable.from(plugins.getInfos()).filter(jvmPluginPredicate).transform(nameFunction).toList();
        for (String expectedJvmPluginName : expectedJvmPluginNames) {
            assertThat(pluginNames, hasItem(expectedJvmPluginName));
        }

        List<String> pluginDescriptions = FluentIterable.from(plugins.getInfos()).filter(jvmPluginPredicate).transform(descriptionFunction).toList();
        for (String expectedJvmPluginDescription : expectedJvmPluginDescriptions) {
            assertThat(pluginDescriptions, hasItem(expectedJvmPluginDescription));
        }

        FluentIterable<String> jvmUrls = FluentIterable.from(plugins.getInfos())
                .filter(and(jvmPluginPredicate, Predicates.not(sitePluginPredicate)))
                .filter(isNull())
                .transform(urlFunction);
        assertThat(Iterables.size(jvmUrls), is(0));

        List<String> sitePluginNames = FluentIterable.from(plugins.getInfos()).filter(sitePluginPredicate).transform(nameFunction).toList();
        for (String expectedSitePluginName : expectedSitePluginNames) {
            assertThat(sitePluginNames, hasItem(expectedSitePluginName));
        }

        List<String> sitePluginDescriptions = FluentIterable.from(plugins.getInfos()).filter(sitePluginPredicate).transform(descriptionFunction).toList();
        for (String sitePluginDescription : expectedSitePluginDescriptions) {
            assertThat(sitePluginDescriptions, hasItem(sitePluginDescription));
        }

        List<String> sitePluginUrls = FluentIterable.from(plugins.getInfos()).filter(sitePluginPredicate).transform(urlFunction).toList();
        assertThat(sitePluginUrls, not(contains(nullValue())));
    }

    private String startNodeWithPlugins(int nodeId, String ... pluginClassNames) throws URISyntaxException {
        URL resource = SimpleNodesInfoTests.class.getResource("/org/elasticsearch/nodesinfo/node" + Integer.toString(nodeId) + "/");
        ImmutableSettings.Builder settings = settingsBuilder();
        if (resource != null) {
            settings.put("path.plugins", new File(resource.toURI()).getAbsolutePath());
        }

        if (pluginClassNames.length > 0) {
            settings.putArray("plugin.types", pluginClassNames);
        }

        String nodeName = cluster().startNode(settings);

        // We wait for a Green status
        client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();

        String serverNodeId = cluster().getInstance(ClusterService.class, nodeName).state().nodes().localNodeId();
        logger.debug("--> server {} started" + serverNodeId);
        return serverNodeId;
    }


    private Predicate<PluginInfo> jvmPluginPredicate = new Predicate<PluginInfo>() {
        public boolean apply(PluginInfo pluginInfo) {
            return pluginInfo.isJvm();
        }
    };

    private Predicate<PluginInfo> sitePluginPredicate = new Predicate<PluginInfo>() {
        public boolean apply(PluginInfo pluginInfo) {
            return pluginInfo.isSite();
        }
    };

    private Function<PluginInfo, String> nameFunction = new Function<PluginInfo, String>() {
        public String apply(PluginInfo pluginInfo) {
            return pluginInfo.getName();
        }
    };

    private Function<PluginInfo, String> descriptionFunction = new Function<PluginInfo, String>() {
        public String apply(PluginInfo pluginInfo) {
            return pluginInfo.getDescription();
        }
    };

    private Function<PluginInfo, String> urlFunction = new Function<PluginInfo, String>() {
        public String apply(PluginInfo pluginInfo) {
            return pluginInfo.getUrl();
        }
    };
}
