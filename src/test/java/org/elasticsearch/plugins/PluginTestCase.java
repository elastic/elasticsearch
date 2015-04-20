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

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Ignore;

import java.net.URISyntaxException;
import java.net.URL;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * Base class that lets you start a node with plugins.
 */
@Ignore
public abstract class PluginTestCase extends ElasticsearchIntegrationTest {
    
    public String startNodeWithPlugins(Settings nodeSettings, String pluginDir, String ... pluginClassNames) throws URISyntaxException {
        URL resource = getClass().getResource(pluginDir);
        ImmutableSettings.Builder settings = settingsBuilder();
        settings.put(nodeSettings);
        if (resource != null) {
            settings.put("path.plugins", getDataPath(pluginDir).toAbsolutePath());
        }

        if (pluginClassNames.length > 0) {
            settings.putArray("plugin.types", pluginClassNames);
        }

        String nodeName = internalCluster().startNode(settings);

        // We wait for a Green status
        client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();

        return internalCluster().getInstance(ClusterService.class, nodeName).state().nodes().localNodeId();
    }
}
