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
package org.elasticsearch.plugin;

import com.google.common.base.Predicate;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.PluginManager;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.helper.HttpClient;
import org.elasticsearch.rest.helper.HttpClientResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0.0)
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
        URI uri = URI.create(PluginManagerTests.class.getResource("plugin_single_folder.zip").toString());
        downloadAndExtract(pluginName, "file://" + uri.getPath());

        cluster().startNode(SETTINGS);

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testLocalPluginInstallSiteFolder() throws Exception {
        //When we have only a folder in top-level (no files either) but it's called _site, we make it work
        //we can either remove the folder while extracting and then re-add it manually or just leave it as it is
        String pluginName = "plugin-test";
        URI uri = URI.create(PluginManagerTests.class.getResource("plugin_folder_site.zip").toString());
        downloadAndExtract(pluginName, "file://" + uri.getPath());

        String nodeName = cluster().startNode(SETTINGS);

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testLocalPluginWithoutFolders() throws Exception {
        //When we don't have folders at all in the top-level, but only files, we don't modify anything
        String pluginName = "plugin-test";
        URI uri = URI.create(PluginManagerTests.class.getResource("plugin_without_folders.zip").toString());
        downloadAndExtract(pluginName, "file://" + uri.getPath());

        cluster().startNode(SETTINGS);

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testLocalPluginFolderAndFile() throws Exception {
        //When we have a single top-level folder but also files in the top-level, we don't modify anything
        String pluginName = "plugin-test";
        URI uri = URI.create(PluginManagerTests.class.getResource("plugin_folder_file.zip").toString());
        downloadAndExtract(pluginName, "file://" + uri.getPath());

        cluster().startNode(SETTINGS);

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSitePluginWithSourceThrows() throws Exception {
        String pluginName = "plugin-with-source";
        URI uri = URI.create(PluginManagerTests.class.getResource("plugin_with_sourcefiles.zip").toString());
        downloadAndExtract(pluginName, "file://" + uri.getPath());
    }

    /**
     * We build a plugin manager instance which wait only for 30 seconds before
     * raising an ElasticsearchTimeoutException
     */
    private static PluginManager pluginManager(String pluginUrl) {
        Tuple<Settings, Environment> initialSettings = InternalSettingsPreparer.prepareSettings(
                ImmutableSettings.settingsBuilder().build(), false);
        if (!initialSettings.v2().pluginsFile().exists()) {
            FileSystemUtils.mkdirs(initialSettings.v2().pluginsFile());
        }
        return new PluginManager(initialSettings.v2(), pluginUrl, PluginManager.OutputMode.SILENT, TimeValue.timeValueSeconds(30));
    }

    private static void downloadAndExtract(String pluginName, String pluginUrl) throws IOException {
        pluginManager(pluginUrl).downloadAndExtract(pluginName);
    }

    private void assertPluginLoaded(String pluginName) {
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().clear().setPlugins(true).get();
        assertThat(nodesInfoResponse.getNodes().length, equalTo(1));
        assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos(), notNullValue());
        assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos().size(), not(0));

        boolean pluginFound = false;

        for (PluginInfo pluginInfo : nodesInfoResponse.getNodes()[0].getPlugins().getInfos()) {
            if (pluginInfo.getName().equals(pluginName)) {
                pluginFound = true;
                break;
            }
        }

        assertThat(pluginFound, is(true));
    }

    private void assertPluginAvailable(String pluginName) throws InterruptedException {
        HttpServerTransport httpServerTransport = cluster().getInstance(HttpServerTransport.class);
        final HttpClient httpClient = new HttpClient(httpServerTransport.boundAddress().publishAddress());
        logger.info("--> tested http address [{}]", httpServerTransport.info().getAddress());

        //checking that the http connector is working properly
        // We will try it for some seconds as it could happen that the REST interface is not yet fully started
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                HttpClientResponse response = httpClient.request("");
                if (response.errorCode() != RestStatus.OK.getStatus()) {
                    // We want to trace what's going on here before failing the test
                    logger.info("--> error caught [{}], headers [{}]", response.errorCode(), response.getHeaders());
                    logger.info("--> cluster state [{}]", cluster().clusterService().state());
                    return false;
                }
                return true;
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));


        //checking now that the plugin is available
        HttpClientResponse response = httpClient.request("_plugin/" + pluginName + "/");
        assertThat(response, notNullValue());
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
        PluginManager pluginManager = pluginManager("file://".concat(
                URI.create(PluginManagerTests.class.getResource("plugin_with_classfile.zip").toString()).getPath()));

        pluginManager.downloadAndExtract("plugin");
        File[] plugins = pluginManager.getListInstalledPlugins();
        assertThat(plugins, notNullValue());
        assertThat(plugins.length, is(1));
    }

    @Test
    public void testInstallSitePlugin() throws IOException {
        PluginManager pluginManager = pluginManager("file://".concat(
                URI.create(PluginManagerTests.class.getResource("plugin_without_folders.zip").toString()).getPath()));

        pluginManager.downloadAndExtract("plugin-site");
        File[] plugins = pluginManager.getListInstalledPlugins();
        assertThat(plugins, notNullValue());
        assertThat(plugins.length, is(1));

        // We want to check that Plugin Manager moves content to _site
        String pluginDir = PLUGIN_DIR.concat("/plugin-site/_site");
        assertThat(FileSystemUtils.exists(new File(pluginDir)), is(true));
    }


    private void singlePluginInstallAndRemove(String pluginShortName, String pluginCoordinates) throws IOException {
        logger.info("--> trying to download and install [{}]", pluginShortName);
        PluginManager pluginManager = pluginManager(pluginCoordinates);
        try {
            pluginManager.downloadAndExtract(pluginShortName);
            File[] plugins = pluginManager.getListInstalledPlugins();
            assertThat(plugins, notNullValue());
            assertThat(plugins.length, is(1));

            // We remove it
            pluginManager.removePlugin(pluginShortName);
            plugins = pluginManager.getListInstalledPlugins();
            assertThat(plugins, notNullValue());
            assertThat(plugins.length, is(0));
        } catch (IOException e) {
            logger.warn("--> IOException raised while downloading plugin [{}]. Skipping test.", e, pluginShortName);
        } catch (ElasticsearchTimeoutException e) {
            logger.warn("--> timeout exception raised while downloading plugin [{}]. Skipping test.", pluginShortName);
        }
    }

    /**
     * We are ignoring by default these tests as they require to have an internet access
     * To activate the test, use -Dtests.network=true
     * We test regular form: username/reponame/version
     * It should find it in download.elasticsearch.org service
     */
    @Test
    @Network
    public void testInstallPluginWithElasticsearchDownloadService() throws IOException {
        assumeTrue(isDownloadServiceWorking("http://download.elasticsearch.org/", "elasticsearch/ci-test.txt"));
        singlePluginInstallAndRemove("elasticsearch/elasticsearch-transport-thrift/1.5.0", null);
    }

    /**
     * We are ignoring by default these tests as they require to have an internet access
     * To activate the test, use -Dtests.network=true
     * We test regular form: groupId/artifactId/version
     * It should find it in maven central service
     */
    @Test
    @Network
    public void testInstallPluginWithMavenCentral() throws IOException {
        assumeTrue(isDownloadServiceWorking("http://search.maven.org/", "/"));
        singlePluginInstallAndRemove("org.elasticsearch/elasticsearch-transport-thrift/1.5.0", null);
    }

    /**
     * We are ignoring by default these tests as they require to have an internet access
     * To activate the test, use -Dtests.network=true
     * We test site plugins from github: userName/repoName
     * It should find it on github
     */
    @Test
    @Network
    public void testInstallPluginWithGithub() throws IOException {
        assumeTrue(isDownloadServiceWorking("https://github.com/", "/"));
        singlePluginInstallAndRemove("elasticsearch/kibana", null);
    }

    private boolean isDownloadServiceWorking(String url, String resource) {
        HttpClient client = new HttpClient(url);
        try {
            if (client.request(resource).errorCode() != 200) {
                logger.warn("[{}{}] download service is not working. Disabling current test.", url, resource);
                return false;
            }
            return true;
        } catch (Throwable t) {
            logger.warn("[{}{}] download service is not working. Disabling current test.", url, resource);
        }
        return false;
    }

    private void deletePluginsFolder() {
        FileSystemUtils.deleteRecursively(new File(PLUGIN_DIR));
    }

    @Test
    public void testRemovePlugin() throws Exception {
        // We want to remove plugin with plugin short name
        singlePluginInstallAndRemove("plugintest", "file://".concat(
                URI.create(PluginManagerTests.class.getResource("plugin_without_folders.zip").toString()).getPath()));

        // We want to remove plugin with groupid/artifactid/version form
        singlePluginInstallAndRemove("groupid/plugintest/1.0.0", "file://".concat(
                URI.create(PluginManagerTests.class.getResource("plugin_without_folders.zip").toString()).getPath()));

        // We want to remove plugin with groupid/artifactid form
        singlePluginInstallAndRemove("groupid/plugintest", "file://".concat(
                URI.create(PluginManagerTests.class.getResource("plugin_without_folders.zip").toString()).getPath()));
    }

    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void testRemovePluginWithURLForm() throws Exception {
        PluginManager pluginManager = pluginManager(null);
        pluginManager.removePlugin("file://whatever");
    }
}
