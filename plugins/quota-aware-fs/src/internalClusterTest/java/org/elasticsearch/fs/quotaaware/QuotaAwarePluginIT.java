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
package org.elasticsearch.fs.quotaaware;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;

public class QuotaAwarePluginIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(QuotaAwareFsPlugin.class);
    }

    /**
     * Check that the plugin is not active, since it is a bootstrap-only plugin.
     */
    public void testPluginShouldNotBeActive() {
        final Client client = client();

        final NodesInfoResponse response = client.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet();

        final boolean hasPlugin = response.getNodes()
            .stream()
            .flatMap(node -> node.getInfo(PluginsAndModules.class).getPluginInfos().stream())
            .noneMatch(pluginInfo -> pluginInfo.getName().equals("quota-aware-fs"));

        assertFalse("quota-aware-fs plugin should not be active", hasPlugin);
    }
}
