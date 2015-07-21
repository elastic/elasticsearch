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
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolTestCase.CaptureOutputTerminal;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.cli.CliTool.ExitStatus.USAGE;
import static org.elasticsearch.common.cli.CliToolTestCase.args;
import static org.elasticsearch.common.io.FileSystemUtilsTests.assertFileContent;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertDirectoryExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileExists;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0.0)
@LuceneTestCase.SuppressFileSystems("*") // TODO: clean up this test to allow extra files
// TODO: jimfs is really broken here (throws wrong exception from detection method).
// if its in your classpath, then do not use plugins!!!!!!
public class PluginManagerTests extends ElasticsearchIntegrationTest {

    private Tuple<Settings, Environment> initialSettings;
    private CaptureOutputTerminal terminal = new CaptureOutputTerminal();

    @Before
    public void setup() throws Exception {
        initialSettings = buildInitialSettings();
        System.setProperty("es.default.path.home", initialSettings.v1().get("path.home"));
        Path binDir = initialSettings.v2().homeFile().resolve("bin");
        if (!Files.exists(binDir)) {
            Files.createDirectories(binDir);
        }
        Path configDir = initialSettings.v2().configFile();
        if (!Files.exists(configDir)) {
            Files.createDirectories(configDir);
        }

    }

    @After
    public void clearPathHome() {
        System.clearProperty("es.default.path.home");
    }

    @Test
    public void testThatPluginNameMustBeSupplied() throws IOException {
        String pluginUrl = getPluginUrlForResource("plugin_single_folder.zip");
        assertStatus("install --url " + pluginUrl, USAGE);
    }

    @Test
    public void testLocalPluginInstallSingleFolder() throws Exception {
        //When we have only a folder in top-level (no files either) we remove that folder while extracting
        String pluginName = "plugin-test";
        String pluginUrl = getPluginUrlForResource("plugin_single_folder.zip");
        String installCommand = String.format(Locale.ROOT, "install %s --url %s", pluginName, pluginUrl);
        assertStatusOk(installCommand);

        internalCluster().startNode(initialSettings.v1());
        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testLocalPluginInstallWithBinAndConfig() throws Exception {
        String pluginName = "plugin-test";
        Environment env = initialSettings.v2();
        Path binDir = env.homeFile().resolve("bin");
        Path pluginBinDir = binDir.resolve(pluginName);

        Path pluginConfigDir = env.configFile().resolve(pluginName);
        String pluginUrl = getPluginUrlForResource("plugin_with_bin_and_config.zip");
        assertStatusOk("install " + pluginName + " --url " + pluginUrl + " --verbose");

        terminal.getTerminalOutput().clear();
        assertStatusOk("list");
        assertThat(terminal.getTerminalOutput(), hasItem(containsString(pluginName)));

        assertDirectoryExists(pluginBinDir);
        assertDirectoryExists(pluginConfigDir);
        Path toolFile = pluginBinDir.resolve("tool");
        assertFileExists(toolFile);

        // check that the file is marked executable, without actually checking that we can execute it.
        PosixFileAttributeView view = Files.getFileAttributeView(toolFile, PosixFileAttributeView.class);
        // the view might be null, on e.g. windows, there is nothing to check there!
        if (view != null) {
            PosixFileAttributes attributes = view.readAttributes();
            assertThat(attributes.permissions(), hasItem(PosixFilePermission.OWNER_EXECUTE));
            assertThat(attributes.permissions(), hasItem(PosixFilePermission.OWNER_READ));
        }
    }

    /**
     * Test for #7890
     */
    @Test
    public void testLocalPluginInstallWithBinAndConfigInAlreadyExistingConfigDir_7890() throws Exception {
        String pluginName = "plugin-test";
        Environment env = initialSettings.v2();
        Path pluginConfigDir = env.configFile().resolve(pluginName);

        assertStatusOk(String.format(Locale.ROOT, "install %s --url %s --verbose", pluginName, getPluginUrlForResource("plugin_with_config_v1.zip")));

        /*
        First time, our plugin contains:
        - config/test.txt (version1)
         */
        assertFileContent(pluginConfigDir, "test.txt", "version1\n");

        // We now remove the plugin
        assertStatusOk("remove " + pluginName);

        // We should still have test.txt
        assertFileContent(pluginConfigDir, "test.txt", "version1\n");

        // Installing a new plugin version
        /*
        Second time, our plugin contains:
        - config/test.txt (version2)
        - config/dir/testdir.txt (version1)
        - config/dir/subdir/testsubdir.txt (version1)
         */
        assertStatusOk(String.format(Locale.ROOT, "install %s --url %s --verbose", pluginName, getPluginUrlForResource("plugin_with_config_v2.zip")));

        assertFileContent(pluginConfigDir, "test.txt", "version1\n");
        assertFileContent(pluginConfigDir, "test.txt.new", "version2\n");
        assertFileContent(pluginConfigDir, "dir/testdir.txt", "version1\n");
        assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt", "version1\n");

        // Removing
        assertStatusOk("remove " + pluginName);
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
        assertStatusOk(String.format(Locale.ROOT, "install %s --url %s --verbose", pluginName, getPluginUrlForResource("plugin_with_config_v3.zip")));

        assertFileContent(pluginConfigDir, "test.txt", "version1\n");
        assertFileContent(pluginConfigDir, "test2.txt", "version1\n");
        assertFileContent(pluginConfigDir, "test.txt.new", "version3\n");
        assertFileContent(pluginConfigDir, "dir/testdir.txt", "version1\n");
        assertFileContent(pluginConfigDir, "dir/testdir.txt.new", "version2\n");
        assertFileContent(pluginConfigDir, "dir/testdir2.txt", "version1\n");
        assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt", "version1\n");
        assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt.new", "version2\n");
    }

    // For #7152
    @Test
    public void testLocalPluginInstallWithBinOnly_7152() throws Exception {
        String pluginName = "plugin-test";
        Environment env = initialSettings.v2();
        Path binDir = env.homeFile().resolve("bin");
        Path pluginBinDir = binDir.resolve(pluginName);

        assertStatusOk(String.format(Locale.ROOT, "install %s --url %s --verbose", pluginName, getPluginUrlForResource("plugin_with_bin_only.zip")));
        assertThatPluginIsListed(pluginName);
        assertDirectoryExists(pluginBinDir);
    }

    @Test
    public void testLocalPluginInstallSiteFolder() throws Exception {
        //When we have only a folder in top-level (no files either) but it's called _site, we make it work
        //we can either remove the folder while extracting and then re-add it manually or just leave it as it is
        String pluginName = "plugin-test";
        assertStatusOk(String.format(Locale.ROOT, "install %s --url %s --verbose", pluginName, getPluginUrlForResource("plugin_folder_site.zip")));

        internalCluster().startNode(initialSettings.v1());

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testLocalPluginWithoutFolders() throws Exception {
        //When we don't have folders at all in the top-level, but only files, we don't modify anything
        String pluginName = "plugin-test";
        assertStatusOk(String.format(Locale.ROOT, "install %s --url %s --verbose", pluginName, getPluginUrlForResource("plugin_without_folders.zip")));

        internalCluster().startNode(initialSettings.v1());

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testLocalPluginFolderAndFile() throws Exception {
        //When we have a single top-level folder but also files in the top-level, we don't modify anything
        String pluginName = "plugin-test";
        assertStatusOk(String.format(Locale.ROOT, "install %s --url %s --verbose", pluginName, getPluginUrlForResource("plugin_folder_file.zip")));

        internalCluster().startNode(initialSettings.v1());

        assertPluginLoaded(pluginName);
        assertPluginAvailable(pluginName);
    }

    @Test
    public void testSitePluginWithSourceDoesNotInstall() throws Exception {
        String pluginName = "plugin-with-source";
        String cmd = String.format(Locale.ROOT, "install %s --url %s --verbose", pluginName, getPluginUrlForResource("plugin_with_sourcefiles.zip"));
        int status = new PluginManagerCliParser(terminal).execute(args(cmd));
        assertThat(status, is(USAGE.status()));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Plugin installation assumed to be site plugin, but contains source code, aborting installation")));
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
        assertStatusOk("list");
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("No plugin detected")));
    }

    @Test
    public void testListInstalledEmptyWithExistingPluginDirectory() throws IOException {
        Files.createDirectory(initialSettings.v2().pluginsFile());
        assertStatusOk("list");
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("No plugin detected")));
    }

    @Test
    public void testInstallPlugin() throws IOException {
        String pluginName = "plugin-classfile";
        assertStatusOk(String.format(Locale.ROOT, "install %s --url %s --verbose", pluginName, getPluginUrlForResource("plugin_with_classfile.zip")));
        assertThatPluginIsListed("plugin-classfile");
    }

    @Test
    public void testInstallSitePlugin() throws IOException {
        String pluginName = "plugin-site";
        assertStatusOk(String.format(Locale.ROOT, "install %s --url %s --verbose", pluginName, getPluginUrlForResource("plugin_without_folders.zip")));
        assertThatPluginIsListed(pluginName);
        // We want to check that Plugin Manager moves content to _site
        assertFileExists(initialSettings.v2().pluginsFile().resolve("plugin-site/_site"));
    }


    private void singlePluginInstallAndRemove(String pluginDescriptor, String pluginName, String pluginCoordinates) throws IOException {
        logger.info("--> trying to download and install [{}]", pluginDescriptor);
        if (pluginCoordinates == null) {
            assertStatusOk(String.format(Locale.ROOT, "install %s --verbose", pluginDescriptor));
        } else {
            assertStatusOk(String.format(Locale.ROOT, "install %s --url %s --verbose", pluginDescriptor, pluginCoordinates));
        }
        assertThatPluginIsListed(pluginName);

        terminal.getTerminalOutput().clear();
        assertStatusOk("remove " + pluginDescriptor);
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Removing " + pluginDescriptor)));

        // not listed anymore
        terminal.getTerminalOutput().clear();
        assertStatusOk("list");
        assertThat(terminal.getTerminalOutput(), not(hasItem(containsString(pluginName))));
    }

    /**
     * We are ignoring by default these tests as they require to have an internet access
     * To activate the test, use -Dtests.network=true
     * We test regular form: username/reponame/version
     * It should find it in download.elasticsearch.org service
     */
    @Test
    @Network
    @AwaitsFix(bugUrl = "fails with jar hell failures - http://build-us-00.elastic.co/job/es_core_master_oracle_6/519/testReport/")
    public void testInstallPluginWithElasticsearchDownloadService() throws IOException {
        assumeTrue("download.elastic.co is accessible", isDownloadServiceWorking("download.elastic.co", 80, "/elasticsearch/ci-test.txt"));
        singlePluginInstallAndRemove("elasticsearch/elasticsearch-transport-thrift/2.4.0", "elasticsearch-transport-thrift", null);
    }

    /**
     * We are ignoring by default these tests as they require to have an internet access
     * To activate the test, use -Dtests.network=true
     * We test regular form: groupId/artifactId/version
     * It should find it in maven central service
     */
    @Test
    @Network
    @AwaitsFix(bugUrl = "fails with jar hell failures - http://build-us-00.elastic.co/job/es_core_master_oracle_6/519/testReport/")
    public void testInstallPluginWithMavenCentral() throws IOException {
        assumeTrue("search.maven.org is accessible", isDownloadServiceWorking("search.maven.org", 80, "/"));
        assumeTrue("repo1.maven.org is accessible", isDownloadServiceWorking("repo1.maven.org", 443, "/maven2/org/elasticsearch/elasticsearch-transport-thrift/2.4.0/elasticsearch-transport-thrift-2.4.0.pom"));
        singlePluginInstallAndRemove("org.elasticsearch/elasticsearch-transport-thrift/2.4.0", "elasticsearch-transport-thrift", null);
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
        singlePluginInstallAndRemove("elasticsearch/kibana", "kibana", null);
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
        singlePluginInstallAndRemove("plugintest", "plugintest", getPluginUrlForResource("plugin_without_folders.zip"));

        // We want to remove plugin with groupid/artifactid/version form
        singlePluginInstallAndRemove("groupid/plugintest/1.0.0", "plugintest", getPluginUrlForResource("plugin_without_folders.zip"));

        // We want to remove plugin with groupid/artifactid form
        singlePluginInstallAndRemove("groupid/plugintest", "plugintest", getPluginUrlForResource("plugin_without_folders.zip"));
    }

    @Test
    public void testRemovePlugin_NullName_ThrowsException() throws IOException {
        int status = new PluginManagerCliParser(terminal).execute(args("remove "));
        assertThat("Terminal output was: " + terminal.getTerminalOutput(), status, is(USAGE.status()));
    }

    @Test
    public void testRemovePluginWithURLForm() throws Exception {
        int status = new PluginManagerCliParser(terminal).execute(args("remove file://whatever"));
        assertThat(terminal.getTerminalOutput(), hasItem(containsString("Illegal plugin name")));
        assertThat("Terminal output was: " + terminal.getTerminalOutput(), status, is(USAGE.status()));
    }

    @Test
    public void testForbiddenPluginNames() throws IOException {
        assertStatus("remove elasticsearch", USAGE);
        assertStatus("remove elasticsearch.bat", USAGE);
        assertStatus("remove elasticsearch.in.sh", USAGE);
        assertStatus("remove plugin", USAGE);
        assertStatus("remove plugin.bat", USAGE);
        assertStatus("remove service.bat", USAGE);
        assertStatus("remove ELASTICSEARCH", USAGE);
        assertStatus("remove ELASTICSEARCH.IN.SH", USAGE);
    }

    @Test
    public void testOfficialPluginName_ThrowsException() throws IOException {
        PluginManager.checkForOfficialPlugins("elasticsearch-analysis-icu");
        PluginManager.checkForOfficialPlugins("elasticsearch-analysis-kuromoji");
        PluginManager.checkForOfficialPlugins("elasticsearch-analysis-phonetic");
        PluginManager.checkForOfficialPlugins("elasticsearch-analysis-smartcn");
        PluginManager.checkForOfficialPlugins("elasticsearch-analysis-stempel");
        PluginManager.checkForOfficialPlugins("elasticsearch-cloud-aws");
        PluginManager.checkForOfficialPlugins("elasticsearch-cloud-azure");
        PluginManager.checkForOfficialPlugins("elasticsearch-cloud-gce");
        PluginManager.checkForOfficialPlugins("elasticsearch-delete-by-query");
        PluginManager.checkForOfficialPlugins("elasticsearch-lang-javascript");
        PluginManager.checkForOfficialPlugins("elasticsearch-lang-python");

        try {
            PluginManager.checkForOfficialPlugins("elasticsearch-mapper-attachment");
            fail("elasticsearch-mapper-attachment should not be allowed");
        } catch (IllegalArgumentException e) {
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

    private Tuple<Settings, Environment> buildInitialSettings() throws IOException {
        Settings settings = settingsBuilder()
                .put("discovery.zen.ping.multicast.enabled", false)
                .put("http.enabled", true)
                .put("path.home", createTempDir()).build();
        return InternalSettingsPreparer.prepareSettings(settings, false);
    }

    private void assertStatusOk(String command) {
        assertStatus(command, CliTool.ExitStatus.OK);
    }

    private void assertStatus(String command, CliTool.ExitStatus exitStatus) {
        int status = new PluginManagerCliParser(terminal).execute(args(command));
        assertThat("Terminal output was: " + terminal.getTerminalOutput(), status, is(exitStatus.status()));
    }

    private void assertThatPluginIsListed(String pluginName) {
        terminal.getTerminalOutput().clear();
        assertStatusOk("list");
        String message = String.format(Locale.ROOT, "Terminal output was: %s", terminal.getTerminalOutput());
        assertThat(message, terminal.getTerminalOutput(), hasItem(containsString(pluginName)));
    }
}
