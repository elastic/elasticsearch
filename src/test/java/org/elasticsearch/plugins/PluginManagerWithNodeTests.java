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

import com.google.common.base.Predicate;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0.0)
public class PluginManagerWithNodeTests extends PluginManagerIntegrationTestCase {

    private static final Settings SETTINGS = ImmutableSettings.settingsBuilder()
            .put("force.http.enabled", true)
            .build();

    @Test
    public void testLocalPluginInstallSingleFolder() throws Exception {
        //When we have only a folder in top-level (no files either) we remove that folder while extracting
        installPlugin("plugin_single_folder.zip", PLUGIN_NAME);
        internalCluster().startNode(settingsBuilder
                .put(SETTINGS)
                .build());

        assertPluginLoaded(PLUGIN_NAME);
        assertPluginAvailable(PLUGIN_NAME);
    }

    @Test
    public void testLocalPluginInstallSiteFolder() throws Exception {
        //When we have only a folder in top-level (no files either) but it's called _site, we make it work
        //we can either remove the folder while extracting and then re-add it manually or just leave it as it is
        installPlugin("plugin_folder_site.zip", PLUGIN_NAME);
        internalCluster().startNode(settingsBuilder
                .put(SETTINGS)
                .build());

        assertPluginLoaded(PLUGIN_NAME);
        assertPluginAvailable(PLUGIN_NAME);
    }

    @Test
    public void testLocalPluginWithoutFolders() throws Exception {
        //When we don't have folders at all in the top-level, but only files, we don't modify anything
        installPlugin("plugin_without_folders.zip", PLUGIN_NAME);
        internalCluster().startNode(settingsBuilder
                .put(SETTINGS)
                .build());

        assertPluginLoaded(PLUGIN_NAME);
        assertPluginAvailable(PLUGIN_NAME);
    }

    @Test
    public void testLocalPluginFolderAndFile() throws Exception {
        //When we have a single top-level folder but also files in the top-level, we don't modify anything
        installPlugin("plugin_folder_file.zip", PLUGIN_NAME);
        internalCluster().startNode(settingsBuilder
                .put(SETTINGS)
                .build());

        assertPluginLoaded(PLUGIN_NAME);
        assertPluginAvailable(PLUGIN_NAME);
    }

    private void assertPluginLoaded(String pluginName) {
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().clear().setPlugins(true).get();
        assertThat(nodesInfoResponse.getNodes(), not(emptyArray()));
        assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos(), notNullValue());
        assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos(), not(empty()));

        boolean pluginFound = false;

        for (PluginInfo pluginInfo : nodesInfoResponse.getNodes()[0].getPlugins().getInfos()) {
            if (pluginInfo.getName().equals(pluginName)) {
                pluginFound = true;
                break;
            }
        }

        assertThat(pluginFound, is(true));
    }

    private void assertPluginAvailable(String pluginName) throws InterruptedException, IOException {
        final HttpRequestBuilder httpRequestBuilder = getHttpRequestBuilder();

        //checking that the http connector is working properly
        // We will try it for some seconds as it could happen that the REST interface is not yet fully started
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                try {
                    HttpResponse response = httpRequestBuilder.method("GET").path("/").execute();
                    if (response.getStatusCode() != RestStatus.OK.getStatus()) {
                        // We want to trace what's going on here before failing the test
                        logger.info("--> error caught [{}], headers [{}]", response.getStatusCode(), response.getHeaders());
                        logger.info("--> cluster state [{}]", internalCluster().clusterService().state());
                        return false;
                    }
                    return true;
                } catch (IOException e) {
                    throw new ElasticsearchException("HTTP problem", e);
                }
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));


        //checking now that the plugin is available
        HttpResponse response = getHttpRequestBuilder().method("GET").path("/_plugin/" + pluginName + "/").execute();
        assertThat(response, notNullValue());
        assertThat(response.getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    private HttpRequestBuilder getHttpRequestBuilder() {
        return new HttpRequestBuilder(HttpClients.createDefault()).httpTransport(internalCluster().getDataNodeInstance(HttpServerTransport.class));
    }
}
