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

package org.elasticsearch.plugin;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.PluginManager;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.helper.HttpClient;
import org.elasticsearch.rest.helper.HttpClientResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.hamcrest.CoreMatchers.*;

@ClusterScope(scope = Scope.TEST, numNodes = 0)
public class PluginManagerTests extends ElasticsearchIntegrationTest {
    private static final Settings SETTINGS = ImmutableSettings.settingsBuilder()
            .put("discovery.zen.ping.multicast.enabled", false)
            .put("force.http.enabled", true)
            .build();
    private static final String PLUGIN_DIR = "plugins";

    @After
    public void afterTest() {
        deletePluginsFolder();
    }

    @Before
    public void beforeTest() {
        deletePluginsFolder();
    }

    @Test
    public void testLocalPluginInstallSingleFolder() throws Exception {
        //When we have only a folder in top-level (no files either) we remove that folder while extracting
        String pluginName = "plugin-test";
        URL url = PluginManagerTests.class.getResource("plugin_single_folder.zip");
        downloadAndExtract(pluginName, "file://" + url.getFile());

        String nodeName = cluster().startNode(SETTINGS);

        assertPluginLoaded(pluginName);
        assertPluginAvailable(nodeName, pluginName);
    }

    @Test
    public void testLocalPluginInstallSiteFolder() throws Exception {
        //When we have only a folder in top-level (no files either) but it's called _site, we make it work
        //we can either remove the folder while extracting and then re-add it manually or just leave it as it is
        String pluginName = "plugin-test";
        URL url = PluginManagerTests.class.getResource("plugin_folder_site.zip");
        downloadAndExtract(pluginName, "file://" + url.getFile());

        String nodeName = cluster().startNode(SETTINGS);

        assertPluginLoaded(pluginName);
        assertPluginAvailable(nodeName, pluginName);
    }

    @Test
    public void testLocalPluginWithoutFolders() throws Exception {
        //When we don't have folders at all in the top-level, but only files, we don't modify anything
        String pluginName = "plugin-test";
        URL url = PluginManagerTests.class.getResource("plugin_without_folders.zip");
        downloadAndExtract(pluginName, "file://" + url.getFile());

        String nodeName = cluster().startNode(SETTINGS);

        assertPluginLoaded(pluginName);
        assertPluginAvailable(nodeName, pluginName);
    }

    @Test
    public void testLocalPluginFolderAndFile() throws Exception {
        //When we have a single top-level folder but also files in the top-level, we don't modify anything
        String pluginName = "plugin-test";
        URL url = PluginManagerTests.class.getResource("plugin_folder_file.zip");
        downloadAndExtract(pluginName, "file://" + url.getFile());

        String nodeName = cluster().startNode(SETTINGS);

        assertPluginLoaded(pluginName);
        assertPluginAvailable(nodeName, pluginName);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSitePluginWithSourceThrows() throws Exception {
        String pluginName = "plugin-with-source";
        URL url = PluginManagerTests.class.getResource("plugin_with_sourcefiles.zip");
        downloadAndExtract(pluginName, "file://" + url.getFile());
    }

    private static PluginManager pluginManager(String pluginUrl) {
        Tuple<Settings, Environment> initialSettings = InternalSettingsPreparer.prepareSettings(
                ImmutableSettings.settingsBuilder().build(), false);
        if (!initialSettings.v2().pluginsFile().exists()) {
            FileSystemUtils.mkdirs(initialSettings.v2().pluginsFile());
        }
        return new PluginManager(initialSettings.v2(), pluginUrl, PluginManager.OutputMode.SILENT);
    }

    private static void downloadAndExtract(String pluginName, String pluginUrl) throws IOException {
        pluginManager(pluginUrl).downloadAndExtract(pluginName);
    }

    private void assertPluginLoaded(String pluginName) {
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().clear().setPlugin(true).get();
        assertThat(nodesInfoResponse.getNodes().length, equalTo(1));
        assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos(), notNullValue());
        assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos().size(), equalTo(1));
        assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos().get(0).getName(), equalTo(pluginName));
        assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos().get(0).isSite(), equalTo(true));
    }

    private void assertPluginAvailable(String nodeName, String pluginName) {
        HttpServerTransport httpServerTransport = cluster().getInstance(HttpServerTransport.class);
        HttpClient httpClient = new HttpClient(httpServerTransport.boundAddress().publishAddress());
        //checking that the http connector is working properly
        HttpClientResponse response = httpClient.request("");
        assertThat(response.errorCode(), equalTo(RestStatus.OK.getStatus()));
        assertThat(response.response(), containsString(nodeName));
        //checking now that the plugin is available
        response = httpClient.request("_plugin/" + pluginName + "/");
        assertThat(response.errorCode(), equalTo(RestStatus.OK.getStatus()));
    }

    @Test
    public void testListInstalledEmpty() throws IOException {
        File[] plugins = pluginManager(null).getListInstalledPlugins();
        assertThat(plugins, notNullValue());
        assertThat(plugins.length, is(0));
    }

    @Test(expected = IOException.class)
    public void testInstallPluginNull() throws IOException {
        pluginManager(null).downloadAndExtract("");
    }


    @Test
    public void testInstallPlugin() throws IOException {
        PluginManager pluginManager = pluginManager("file://".concat(PluginManagerTests.class.getResource("plugin_with_classfile.zip").getFile()));

        pluginManager.downloadAndExtract("plugin");
        File[] plugins = pluginManager.getListInstalledPlugins();
        assertThat(plugins, notNullValue());
        assertThat(plugins.length, is(1));
    }

    @Test
    public void testInstallSitePlugin() throws IOException {
        PluginManager pluginManager = pluginManager("file://".concat(PluginManagerTests.class.getResource("plugin_without_folders.zip").getFile()));

        pluginManager.downloadAndExtract("plugin-site");
        File[] plugins = pluginManager.getListInstalledPlugins();
        assertThat(plugins, notNullValue());
        assertThat(plugins.length, is(1));

        // We want to check that Plugin Manager moves content to _site
        String pluginDir = PLUGIN_DIR.concat("/plugin-site/_site");
        assertThat(FileSystemUtils.exists(new File(pluginDir)), is(true));
    }


    private void singlePluginInstallAndRemove(String pluginShortName, String pluginCoordinates) throws IOException {
        PluginManager pluginManager = pluginManager(pluginCoordinates);
        pluginManager.downloadAndExtract(pluginShortName);
        File[] plugins = pluginManager.getListInstalledPlugins();
        assertThat(plugins, notNullValue());
        assertThat(plugins.length, is(1));

        // We remove it
        pluginManager.removePlugin(pluginShortName);
        plugins = pluginManager.getListInstalledPlugins();
        assertThat(plugins, notNullValue());
        assertThat(plugins.length, is(0));
    }

    /**
     * We are ignoring by default these tests as they require to have an internet access
     * To activate the test, use -Dtests.network=true
     */
    @Test
    @Network
    public void testInstallPluginWithInternet() throws IOException {
        // We test regular form: username/reponame/version
        // It should find it in download.elasticsearch.org service
        singlePluginInstallAndRemove("elasticsearch/elasticsearch-transport-thrift/1.5.0", null);

        // We test regular form: groupId/artifactId/version
        // It should find it in maven central service
        singlePluginInstallAndRemove("org.elasticsearch/elasticsearch-transport-thrift/1.5.0", null);

        // We test site plugins from github: userName/repoName
        // It should find it on github
        singlePluginInstallAndRemove("elasticsearch/kibana", null);
    }

    private void deletePluginsFolder() {
        FileSystemUtils.deleteRecursively(new File(PLUGIN_DIR));
    }

    @Test
    public void testRemovePlugin() throws Exception {
        // We want to remove plugin with plugin short name
        singlePluginInstallAndRemove("plugintest", "file://".concat(PluginManagerTests.class.getResource("plugin_without_folders.zip").getFile()));

        // We want to remove plugin with groupid/artifactid/version form
        singlePluginInstallAndRemove("groupid/plugintest/1.0.0", "file://".concat(PluginManagerTests.class.getResource("plugin_without_folders.zip").getFile()));

        // We want to remove plugin with groupid/artifactid form
        singlePluginInstallAndRemove("groupid/plugintest", "file://".concat(PluginManagerTests.class.getResource("plugin_without_folders.zip").getFile()));
    }

    @Test(expected = ElasticSearchIllegalArgumentException.class)
    public void testRemovePluginWithURLForm() throws Exception {
        PluginManager pluginManager = pluginManager(null);
        pluginManager.removePlugin("file://whatever");
    }
}
