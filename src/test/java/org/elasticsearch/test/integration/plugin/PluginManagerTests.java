/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.test.integration.plugin;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPerparer;
import org.elasticsearch.plugins.PluginManager;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.elasticsearch.test.integration.rest.helper.HttpClient;
import org.elasticsearch.test.integration.rest.helper.HttpClientResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

public class PluginManagerTests extends AbstractNodesTests {

    private static final String PLUGIN_DIR = "plugins";
    private static final String NODE_NAME = "plugin-test-node";

    @Test
    public void testLocalPluginInstallSingleFolder() throws Exception {
        //When we have only a folder in top-level (no files either) we remove that folder while extracting
        String pluginName = "plugin-test";
        URL url = PluginManagerTests.class.getResource("plugin_single_folder.zip");
        downloadAndExtract(pluginName, "file://" + url.getFile());

        startNode();

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testLocalPluginInstallSiteFolder() throws Exception {
        //When we have only a folder in top-level (no files either) but it's called _site, we make it work
        //we can either remove the folder while extracting and then re-add it manually or just leave it as it is
        String pluginName = "plugin-test";
        URL url = PluginManagerTests.class.getResource("plugin_folder_site.zip");
        downloadAndExtract(pluginName, "file://" + url.getFile());

        startNode();

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testLocalPluginWithoutFolders() throws Exception {
        //When we don't have folders at all in the top-level, but only files, we don't modify anything
        String pluginName = "plugin-test";
        URL url = PluginManagerTests.class.getResource("plugin_without_folders.zip");
        downloadAndExtract(pluginName, "file://" + url.getFile());

        startNode();

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testLocalPluginFolderAndFile() throws Exception {
        //When we have a single top-level folder but also files in the top-level, we don't modify anything
        String pluginName = "plugin-test";
        URL url = PluginManagerTests.class.getResource("plugin_folder_file.zip");
        downloadAndExtract(pluginName, "file://" + url.getFile());

        startNode();

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    private static void downloadAndExtract(String pluginName, String pluginUrl) throws Exception {
        Tuple<Settings, Environment> initialSettings = InternalSettingsPerparer.prepareSettings(
                ImmutableSettings.settingsBuilder()/*.put("path.plugins", PLUGIN_DIR)*/.build(), false);
        if (!initialSettings.v2().pluginsFile().exists()) {
            FileSystemUtils.mkdirs(initialSettings.v2().pluginsFile());
        }
        PluginManager pluginManager = new PluginManager(initialSettings.v2(), pluginUrl);
        pluginManager.downloadAndExtract(pluginName, false);
    }

    private void startNode() {
        startNode(NODE_NAME, ImmutableSettings.settingsBuilder()
                .put("discovery.zen.ping.multicast.enabled", false)
                /*.put("path.plugins", PLUGIN_DIR)*/);
    }

    private void assertPluginLoaded(String pluginName) {
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().clear().setPlugin(true).get();
        assertThat(nodesInfoResponse.getNodes().length, equalTo(1));
        assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos(), notNullValue());
        assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos().size(), equalTo(1));
        assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos().get(0).getName(), equalTo(pluginName));
        assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos().get(0).isSite(), equalTo(true));
    }

    private void assertPluginAvailable(String pluginName) {
        HttpClient httpClient = new HttpClient("http://127.0.0.1:9200/");
        //checking that the http connector is working properly
        HttpClientResponse response = httpClient.request("");
        assertThat(response.errorCode(), equalTo(RestStatus.OK.getStatus()));
        assertThat(response.response(), containsString(NODE_NAME));
        //checking now that the plugin is available
        response = httpClient.request("_plugin/" + pluginName + "/");
        assertThat(response.errorCode(), equalTo(RestStatus.OK.getStatus()));
    }

    @Before
    public void before() {
        deletePluginsFolder();
    }

    @After
    public void after() {
        deletePluginsFolder();
        closeAllNodes();
    }

    private void deletePluginsFolder() {
        FileSystemUtils.deleteRecursively(new File(PLUGIN_DIR));
    }
}
