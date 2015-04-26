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
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.io.FileSystemUtilsTests.assertFileContent;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertDirectoryExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileExists;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0.0)
@LuceneTestCase.SuppressFileSystems("*") // TODO: clean up this test to allow extra files
// TODO: jimfs is really broken here (throws wrong exception from detection method).
// if its in your classpath, then do not use plugins!!!!!!
public class PluginManagerTests extends ElasticsearchIntegrationTest {

    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void testDownloadAndExtract_NullName_ThrowsException() throws IOException {
        pluginManager(getPluginUrlForResource("plugin_single_folder.zip")).downloadAndExtract(null);
    }

    @Test
    public void testLocalPluginInstallSingleFolder() throws Exception {
        //When we have only a folder in top-level (no files either) we remove that folder while extracting
        String pluginName = "plugin-test";
        Tuple<Settings, Environment> initialSettings = buildInitialSettings();
        downloadAndExtract(pluginName, initialSettings, getPluginUrlForResource("plugin_single_folder.zip"));

        internalCluster().startNode(initialSettings.v1());

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testLocalPluginInstallWithBinAndConfig() throws Exception {
        String pluginName = "plugin-test";
        Tuple<Settings, Environment> initialSettings = buildInitialSettings();
        Environment env = initialSettings.v2();
        Path binDir = env.homeFile().resolve("bin");
        if (!Files.exists(binDir)) {
            Files.createDirectories(binDir);
        }
        Path pluginBinDir = binDir.resolve(pluginName);
        Path configDir = env.configFile();
        if (!Files.exists(configDir)) {
            Files.createDirectories(configDir);
        }
        Path pluginConfigDir =configDir.resolve(pluginName);
        try {

            PluginManager pluginManager = pluginManager(getPluginUrlForResource("plugin_with_bin_and_config.zip"), initialSettings);

            pluginManager.downloadAndExtract(pluginName);

            Path[] plugins = pluginManager.getListInstalledPlugins();

            assertThat(plugins, arrayWithSize(1));
            assertDirectoryExists(pluginBinDir);
            assertDirectoryExists(pluginConfigDir);
            Path toolFile = pluginBinDir.resolve("tool");
            assertFileExists(toolFile);

            // check that the file is marked executable, without actually checking that we can execute it.
            PosixFileAttributeView view = Files.getFileAttributeView(toolFile, PosixFileAttributeView.class);
            // the view might be null, on e.g. windows, there is nothing to check there!
            if (view != null) {
                PosixFileAttributes attributes = view.readAttributes();
                assertTrue("unexpected permissions: " + attributes.permissions(),
                           attributes.permissions().contains(PosixFilePermission.OWNER_EXECUTE));
            }
        } finally {
            // we need to clean up the copied dirs
            IOUtils.rm(pluginBinDir, pluginConfigDir);
        }
    }

    /**
     * Test for #7890
     */
    @Test
    public void testLocalPluginInstallWithBinAndConfigInAlreadyExistingConfigDir_7890() throws Exception {
        String pluginName = "plugin-test";
        Tuple<Settings, Environment> initialSettings = buildInitialSettings();
        Environment env = initialSettings.v2();

        Path configDir = env.configFile();
        if (!Files.exists(configDir)) {
            Files.createDirectories(configDir);
        }
        Path pluginConfigDir = configDir.resolve(pluginName);

        try {
            PluginManager pluginManager = pluginManager(getPluginUrlForResource("plugin_with_config_v1.zip"), initialSettings);
            pluginManager.downloadAndExtract(pluginName);

            Path[] plugins = pluginManager.getListInstalledPlugins();
            assertThat(plugins, arrayWithSize(1));

            /*
            First time, our plugin contains:
            - config/test.txt (version1)
             */
            assertFileContent(pluginConfigDir, "test.txt", "version1\n");

            // We now remove the plugin
            pluginManager.removePlugin(pluginName);
            // We should still have test.txt
            assertFileContent(pluginConfigDir, "test.txt", "version1\n");

            // Installing a new plugin version
            /*
            Second time, our plugin contains:
            - config/test.txt (version2)
            - config/dir/testdir.txt (version1)
            - config/dir/subdir/testsubdir.txt (version1)
             */
            pluginManager = pluginManager(getPluginUrlForResource("plugin_with_config_v2.zip"), initialSettings);
            pluginManager.downloadAndExtract(pluginName);

            assertFileContent(pluginConfigDir, "test.txt", "version1\n");
            assertFileContent(pluginConfigDir, "test.txt.new", "version2\n");
            assertFileContent(pluginConfigDir, "dir/testdir.txt", "version1\n");
            assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt", "version1\n");

            // Removing
            pluginManager.removePlugin(pluginName);
            assertFileContent(pluginConfigDir, "test.txt", "version1\n");
            assertFileContent(pluginConfigDir, "test.txt.new", "version2\n");
            assertFileContent(pluginConfigDir, "dir/testdir.txt", "version1\n");
            assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt", "version1\n");

            // Installing a new plugin version
            /*
            Third time, our plugin contains:
            - config/test.txt (version3)
            - config/test2.txt (version1)
            - config/dir/testdir.txt (version2)
            - config/dir/testdir2.txt (version1)
            - config/dir/subdir/testsubdir.txt (version2)
             */
            pluginManager = pluginManager(getPluginUrlForResource("plugin_with_config_v3.zip"), initialSettings);
            pluginManager.downloadAndExtract(pluginName);

            assertFileContent(pluginConfigDir, "test.txt", "version1\n");
            assertFileContent(pluginConfigDir, "test2.txt", "version1\n");
            assertFileContent(pluginConfigDir, "test.txt.new", "version3\n");
            assertFileContent(pluginConfigDir, "dir/testdir.txt", "version1\n");
            assertFileContent(pluginConfigDir, "dir/testdir.txt.new", "version2\n");
            assertFileContent(pluginConfigDir, "dir/testdir2.txt", "version1\n");
            assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt", "version1\n");
            assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt.new", "version2\n");
        } finally {
            // we need to clean up the copied dirs
            IOUtils.rm(pluginConfigDir);
        }
    }

    // For #7152
    @Test
    public void testLocalPluginInstallWithBinOnly_7152() throws Exception {
        String pluginName = "plugin-test";
        Tuple<Settings, Environment> initialSettings = buildInitialSettings();
        Environment env = initialSettings.v2();
        Path binDir = env.homeFile().resolve("bin");
        if (!Files.exists(binDir)) {
            Files.createDirectories(binDir);
        }
        Path pluginBinDir = binDir.resolve(pluginName);
        try {
            PluginManager pluginManager = pluginManager(getPluginUrlForResource("plugin_with_bin_only.zip"), initialSettings);
            pluginManager.downloadAndExtract(pluginName);
            Path[] plugins = pluginManager.getListInstalledPlugins();
            assertThat(plugins.length, is(1));
            assertDirectoryExists(pluginBinDir);
        } finally {
            // we need to clean up the copied dirs
            IOUtils.rm(pluginBinDir);
        }
    }

    @Test
    public void testLocalPluginInstallSiteFolder() throws Exception {
        //When we have only a folder in top-level (no files either) but it's called _site, we make it work
        //we can either remove the folder while extracting and then re-add it manually or just leave it as it is
        String pluginName = "plugin-test";
        Tuple<Settings, Environment> initialSettings = buildInitialSettings();
        downloadAndExtract(pluginName, initialSettings, getPluginUrlForResource("plugin_folder_site.zip"));

        internalCluster().startNode(initialSettings.v1());

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testLocalPluginWithoutFolders() throws Exception {
        //When we don't have folders at all in the top-level, but only files, we don't modify anything
        String pluginName = "plugin-test";
        Tuple<Settings, Environment> initialSettings = buildInitialSettings();
        downloadAndExtract(pluginName, initialSettings, getPluginUrlForResource("plugin_without_folders.zip"));

        internalCluster().startNode(initialSettings.v1());

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testLocalPluginFolderAndFile() throws Exception {
        //When we have a single top-level folder but also files in the top-level, we don't modify anything
        String pluginName = "plugin-test";
        Tuple<Settings, Environment> initialSettings = buildInitialSettings();
        downloadAndExtract(pluginName, initialSettings, getPluginUrlForResource("plugin_folder_file.zip"));

        internalCluster().startNode(initialSettings.v1());

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSitePluginWithSourceThrows() throws Exception {
        String pluginName = "plugin-with-source";
        downloadAndExtract(pluginName, buildInitialSettings(), getPluginUrlForResource("plugin_with_sourcefiles.zip"));
    }

    private PluginManager pluginManager(String pluginUrl) throws IOException {
        return pluginManager(pluginUrl, buildInitialSettings());
    }

    private Tuple<Settings, Environment> buildInitialSettings() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
            .put("discovery.zen.ping.multicast.enabled", false)
            .put("http.enabled", true)
            .put("path.home", createTempDir()).build();
        return InternalSettingsPreparer.prepareSettings(settings, false);
    }

    /**
     * We build a plugin manager instance which wait only for 30 seconds before
     * raising an ElasticsearchTimeoutException
     */
    private PluginManager pluginManager(String pluginUrl, Tuple<Settings, Environment> initialSettings) throws IOException {
        if (!Files.exists(initialSettings.v2().pluginsFile())) {
            Files.createDirectories(initialSettings.v2().pluginsFile());
        }
        return new PluginManager(initialSettings.v2(), pluginUrl, PluginManager.OutputMode.VERBOSE, TimeValue.timeValueSeconds(30));
    }

    private void downloadAndExtract(String pluginName, Tuple<Settings, Environment> initialSettings, String pluginUrl) throws IOException {
        pluginManager(pluginUrl, initialSettings).downloadAndExtract(pluginName);
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

    private void assertPluginAvailable(String pluginName) throws InterruptedException, IOException {
        final HttpRequestBuilder httpRequestBuilder = httpClient();

        //checking that the http connector is working properly
        // We will try it for some seconds as it could happen that the REST interface is not yet fully started
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
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
        HttpResponse response = httpClient().method("GET").path("/_plugin/" + pluginName + "/").execute();
        assertThat(response, notNullValue());
        assertThat(response.getReasonPhrase(), response.getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    @Test
    public void testListInstalledEmpty() throws IOException {
        Path[] plugins = pluginManager(null).getListInstalledPlugins();
        assertThat(plugins, notNullValue());
        assertThat(plugins.length, is(0));
    }

    @Test(expected = IOException.class)
    public void testInstallPluginNull() throws IOException {
        pluginManager(null).downloadAndExtract("plugin-test");
    }


    @Test
    public void testInstallPlugin() throws IOException {
        PluginManager pluginManager = pluginManager(getPluginUrlForResource("plugin_with_classfile.zip"));

        pluginManager.downloadAndExtract("plugin-classfile");
        Path[] plugins = pluginManager.getListInstalledPlugins();
        assertThat(plugins, notNullValue());
        assertThat(plugins.length, is(1));
    }

    @Test
    public void testInstallSitePlugin() throws IOException {
        Tuple<Settings, Environment> initialSettings = buildInitialSettings();
        PluginManager pluginManager = pluginManager(getPluginUrlForResource("plugin_without_folders.zip"), initialSettings);

        pluginManager.downloadAndExtract("plugin-site");
        Path[] plugins = pluginManager.getListInstalledPlugins();
        assertThat(plugins, notNullValue());
        assertThat(plugins.length, is(1));

        // We want to check that Plugin Manager moves content to _site
        assertFileExists(initialSettings.v2().pluginsFile().resolve("plugin-site/_site"));
    }


    private void singlePluginInstallAndRemove(String pluginShortName, String pluginCoordinates) throws IOException {
        logger.info("--> trying to download and install [{}]", pluginShortName);
        PluginManager pluginManager = pluginManager(pluginCoordinates);
        try {
            pluginManager.downloadAndExtract(pluginShortName);
            Path[] plugins = pluginManager.getListInstalledPlugins();
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
        assumeTrue("download.elasticsearch.org is accessible", isDownloadServiceWorking("download.elasticsearch.org", 80, "/elasticsearch/ci-test.txt"));
        singlePluginInstallAndRemove("elasticsearch/elasticsearch-transport-thrift/2.4.0", null);
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
        assumeTrue("search.maven.org is accessible", isDownloadServiceWorking("search.maven.org", 80, "/"));
        assumeTrue("repo1.maven.org is accessible", isDownloadServiceWorking("repo1.maven.org", 443, "/maven2/org/elasticsearch/elasticsearch-transport-thrift/2.4.0/elasticsearch-transport-thrift-2.4.0.pom"));
        singlePluginInstallAndRemove("org.elasticsearch/elasticsearch-transport-thrift/2.4.0", null);
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
        assumeTrue("github.com is accessible", isDownloadServiceWorking("github.com", 443, "/"));
        singlePluginInstallAndRemove("elasticsearch/kibana", null);
    }

    private boolean isDownloadServiceWorking(String host, int port, String resource) {
        try {
            String protocol = port == 443 ? "https" : "http";
            HttpResponse response = new HttpRequestBuilder(HttpClients.createDefault()).protocol(protocol).host(host).port(port).path(resource).execute();
            if (response.getStatusCode() != 200) {
                logger.warn("[{}{}] download service is not working. Disabling current test.", host, resource);
                return false;
            }
            return true;
        } catch (Throwable t) {
            logger.warn("[{}{}] download service is not working. Disabling current test.", host, resource);
        }
        return false;
    }

    @Test
    public void testRemovePlugin() throws Exception {
        // We want to remove plugin with plugin short name
        singlePluginInstallAndRemove("plugintest", getPluginUrlForResource("plugin_without_folders.zip"));

        // We want to remove plugin with groupid/artifactid/version form
        singlePluginInstallAndRemove("groupid/plugintest/1.0.0", getPluginUrlForResource("plugin_without_folders.zip"));

        // We want to remove plugin with groupid/artifactid form
        singlePluginInstallAndRemove("groupid/plugintest", getPluginUrlForResource("plugin_without_folders.zip"));
    }

    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void testRemovePlugin_NullName_ThrowsException() throws IOException {
        pluginManager(getPluginUrlForResource("plugin_single_folder.zip")).removePlugin(null);
    }

    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void testRemovePluginWithURLForm() throws Exception {
        PluginManager pluginManager = pluginManager(null);
        pluginManager.removePlugin("file://whatever");
    }

    @Test
    public void testForbiddenPluginName_ThrowsException() throws IOException {
        runTestWithForbiddenName(null);
        runTestWithForbiddenName("");
        runTestWithForbiddenName("elasticsearch");
        runTestWithForbiddenName("elasticsearch.bat");
        runTestWithForbiddenName("elasticsearch.in.sh");
        runTestWithForbiddenName("plugin");
        runTestWithForbiddenName("plugin.bat");
        runTestWithForbiddenName("service.bat");
        runTestWithForbiddenName("ELASTICSEARCH");
        runTestWithForbiddenName("ELASTICSEARCH.IN.SH");
    }

    private void runTestWithForbiddenName(String name) throws IOException {
        try {
            pluginManager(null).removePlugin(name);
            fail("this plugin name [" + name +
                    "] should not be allowed");
        } catch (ElasticsearchIllegalArgumentException e) {
            // We expect that error
        }
    }


    /**
     * Retrieve a URL string that represents the resource with the given {@code resourceName}.
     * @param resourceName The resource name relative to {@link PluginManagerTests}.
     * @return Never {@code null}.
     * @throws NullPointerException if {@code resourceName} does not point to a valid resource.
     */
    private String getPluginUrlForResource(String resourceName) {
        URI uri = URI.create(PluginManagerTests.class.getResource(resourceName).toString());

        return "file://" + uri.getPath();
    }
}
