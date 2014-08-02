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

package org.elasticsearch.plugins;

import com.google.common.collect.Lists;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.nodesinfo.SimpleNodesInfoTests;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.Collections;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 *
 */
@ClusterScope(scope= ElasticsearchIntegrationTest.Scope.TEST, numDataNodes=0, transportClientRatio = 0)
public class PluginLuceneCheckerTests extends ElasticsearchIntegrationTest {

    /**
     * We check that no Lucene version checking is done
     * when we set `"plugins.check_lucene":false`
     */
    @Test
    public void testDisableLuceneVersionCheckingPlugin() throws URISyntaxException {
        String serverNodeId = SimpleNodesInfoTests.startNodeWithPlugins(
                settingsBuilder().put("plugins.check_lucene", false)
                        .put("plugins." + PluginsService.ES_PLUGIN_PROPERTIES_FILE_KEY, "es-plugin-test.properties")
                        .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true).build(),
                "/org/elasticsearch/plugins/lucene/");
        logger.info("--> server {} started" + serverNodeId);

        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().clear().setPlugins(true).execute().actionGet();
        logger.info("--> full json answer, status " + response.toString());

        ElasticsearchAssertions.assertNodeContainsPlugins(response, serverNodeId,
                Lists.newArrayList("old-lucene"), Lists.newArrayList("old"), Lists.newArrayList("1.0.0"), // JVM Plugin
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST);// No Site Plugin
    }

    /**
     * We check that with an old plugin (built on an old Lucene version)
     * plugin is not loaded
     * We check that with a recent plugin (built on current Lucene version)
     * plugin is loaded
     * We check that with a too recent plugin (built on an unknown Lucene version)
     * plugin is not loaded
     */
    @Test
    public void testEnableLuceneVersionCheckingPlugin() throws URISyntaxException {
        String serverNodeId = SimpleNodesInfoTests.startNodeWithPlugins(
                settingsBuilder().put("plugins.check_lucene", true)
                        .put("plugins." + PluginsService.ES_PLUGIN_PROPERTIES_FILE_KEY, "es-plugin-test.properties")
                        .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true).build(),
                "/org/elasticsearch/plugins/lucene/");
        logger.info("--> server {} started" + serverNodeId);

        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().clear().setPlugins(true).execute().actionGet();
        logger.info("--> full json answer, status " + response.toString());

        ElasticsearchAssertions.assertNodeContainsPlugins(response, serverNodeId,
                Lists.newArrayList("current-lucene"), Lists.newArrayList("current"), Lists.newArrayList("2.0.0"), // JVM Plugin
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST);// No Site Plugin
    }
}
