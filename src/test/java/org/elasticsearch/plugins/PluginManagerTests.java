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

import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.http.client.HttpDownloadHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.nio.file.Path;

import static org.elasticsearch.common.io.FileSystemUtilsTests.assertFileContent;
import static org.hamcrest.Matchers.*;

/**
 * Those tests run without any running node
 */
public class PluginManagerTests extends PluginManagerIntegrationTestCase {

    @Test
    public void testPluginWith_SingleFolder() throws Exception {
        testInstallRemovePlugin("plugin_single_folder.zip", 0, 1, null, null);
    }

    @Test
    public void testPluginWith_ClassBinAndConfigFolders() throws Exception {
        testInstallRemovePlugin("plugin_with_bin_and_config.zip", 1, null, 1, 1);
    }

    // For #7152
    @Test
    public void testPluginWith_BinOnly_7152() throws Exception {
        testInstallRemovePlugin("plugin_with_bin_only.zip", 0, null, null, 1);
    }

    // For #7152
    @Test
    public void testPluginWith_ConfigOnly_7152() throws Exception {
        testInstallRemovePlugin("plugin_with_config_only.zip", 0, null, 1, null);
    }

    @Test
    public void testPluginWith_Site() throws Exception {
        testInstallRemovePlugin("plugin_folder_site.zip", 0, 1, null, null);
    }

    @Test
    public void testPluginWithout_Folders() throws Exception {
        testInstallRemovePlugin("plugin_without_folders.zip", 0, 1, null, null);
    }

    @Test
    public void testPluginWith_FolderAndFiles() throws Exception {
        testInstallRemovePlugin("plugin_folder_file.zip", 0, 1, null, null);
    }

    @Test
    public void testPluginWith_SourceFiles() throws Exception {
        installPlugin("plugin_with_sourcefiles.zip", PLUGIN_NAME, CliTool.ExitStatus.DATA_ERROR);
    }

    @Test
    public void testPluginWith_ClassFiles() throws Exception {
        testInstallRemovePlugin("plugin_with_classfile.zip", 1, null, null, null);
    }

    @Test
    public void testLocalPluginInstallWithBinAndConfig() throws Exception {
        testInstallRemovePlugin("plugin_with_bin_and_config.zip", 1, null, 1, 1);
    }

    /**
     * Test for #7890
     */
    @Test
    public void testLocalPluginInstallWithBinAndConfigInAlreadyExistingConfigDir_7890() throws Exception {
        installPlugin("plugin_with_config_v1.zip", PLUGIN_NAME);

        Path pluginConfigDir = configDir.resolve(PLUGIN_NAME);

        /*
        First time, our plugin contains:
        - config/test.txt (version1)
         */
        assertFileContent(pluginConfigDir, "test.txt", "version1\n");

        // We now remove the plugin
        removePlugin(PLUGIN_NAME);

        // We should still have test.txt
        assertFileContent(pluginConfigDir, "test.txt", "version1\n");

        // Installing a new plugin version
        /*
        Second time, our plugin contains:
        - config/test.txt (version2)
        - config/dir/testdir.txt (version1)
        - config/dir/subdir/testsubdir.txt (version1)
         */
        installPlugin("plugin_with_config_v2.zip", PLUGIN_NAME);

        assertFileContent(pluginConfigDir, "test.txt", "version1\n");
        assertFileContent(pluginConfigDir, "test.txt.new", "version2\n");
        assertFileContent(pluginConfigDir, "dir/testdir.txt", "version1\n");
        assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt", "version1\n");

        // Removing
        removePlugin(PLUGIN_NAME);
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
        installPlugin("plugin_with_config_v3.zip", PLUGIN_NAME);

        assertFileContent(pluginConfigDir, "test.txt", "version1\n");
        assertFileContent(pluginConfigDir, "test2.txt", "version1\n");
        assertFileContent(pluginConfigDir, "test.txt.new", "version3\n");
        assertFileContent(pluginConfigDir, "dir/testdir.txt", "version1\n");
        assertFileContent(pluginConfigDir, "dir/testdir.txt.new", "version2\n");
        assertFileContent(pluginConfigDir, "dir/testdir2.txt", "version1\n");
        assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt", "version1\n");
        assertFileContent(pluginConfigDir, "dir/subdir/testsubdir.txt.new", "version2\n");
    }

    @Test
    public void testFailingInstallWith_SilentMode() throws Exception {
        Settings settings = settingsBuilder.build();
        Environment env = new Environment(settings);

        CliToolTestCase.CaptureOutputTerminal silentTerminal = new CliToolTestCase.CaptureOutputTerminal();

        silentTerminal.getTerminalOutput().clear();
        silentTerminal.verbosity(Terminal.Verbosity.SILENT);
        PluginManager.Install install = new PluginManager.Install(silentTerminal, null, "doesnotexist/doesnotexist", TimeValue.timeValueMillis(0));
        CliTool.ExitStatus status = install.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.UNAVAILABLE));

        if (!silentTerminal.getTerminalOutput().isEmpty()) {
            logger.error("we should have no output but we got: {}", silentTerminal.getTerminalOutput());
            fail("output terminal should be empty in silent mode");
        }
    }

    @Test
    public void testListWith_EmptyPluginsFolder() throws Exception {
        Settings settings = settingsBuilder.build();
        Environment env = new Environment(settings);

        captureTerminal.getTerminalOutput().clear();
        PluginManager.ListPlugins list = new PluginManager.ListPlugins(captureTerminal);
        CliTool.ExitStatus status = list.execute(settings, env);
        assertThat(status, is(CliTool.ExitStatus.OK));

        assertThat(captureTerminal.getTerminalOutput(), is(not(empty())));

        // We iterate over terminal and try to find a successful message
        boolean foundSuccess = false;
        for (String output : captureTerminal.getTerminalOutput()) {
            if (output.contains("No plugin detected in")) {
                foundSuccess = true;
                break;
            }
        }

        if (!foundSuccess) {
            logger.info("we should have a `No plugin detected in ...` message but we got {}", captureTerminal.getTerminalOutput());
        }
        assertThat(foundSuccess, is(true));

    }

    @Test
    public void testRemovePlugin() throws Exception {
        // We want to remove plugin with plugin short name
        testInstallRemovePlugin("plugin_without_folders.zip", "plugintest", "plugintest", null, null);

        // We want to remove plugin with groupid/artifactid/version form
        testInstallRemovePlugin("plugin_without_folders.zip", "groupid/plugintest/1.0.0", "plugintest", null, null);

        // We want to remove plugin with groupid/artifactid form
        testInstallRemovePlugin("plugin_without_folders.zip", "groupid/plugintest", "plugintest", null, null);
    }

    @Test
    public void testForbiddenPluginName_ThrowsException() throws Exception {
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


    // Tests requiring internet connection

    /**
     * We are ignoring by default these tests as they require to have an internet access
     * To activate the test, use -Dtests.network=true
     * We test regular form: username/reponame/version
     * It should find it in download.elasticsearch.org service
     */
    @Test
    @Network
    public void testInstallPluginWithElasticsearchDownloadService() throws Exception {
        assumeTrue(isDownloadServiceWorking("download.elasticsearch.org", 80, "/elasticsearch/ci-test.txt"));
        testInstallRemovePlugin(null, "elasticsearch/elasticsearch-transport-thrift/2.4.0", "transport-thrift", null, null);
    }

    /**
     * We are ignoring by default these tests as they require to have an internet access
     * To activate the test, use -Dtests.network=true
     * We test regular form: groupId/artifactId/version
     * It should find it in maven central service
     */
    @Test
    @Network
    public void testInstallPluginWithMavenCentral() throws Exception {
        assumeTrue(isDownloadServiceWorking("search.maven.org", 80, "/"));
        assumeTrue(isDownloadServiceWorking("repo1.maven.org", 443, "/maven2/org/elasticsearch/elasticsearch-transport-thrift/2.4.0/elasticsearch-transport-thrift-2.4.0.pom"));
        testInstallRemovePlugin(null, "org.elasticsearch/elasticsearch-transport-thrift/2.4.0", "transport-thrift", null, null);
    }

    /**
     * We are ignoring by default these tests as they require to have an internet access
     * To activate the test, use -Dtests.network=true
     * We test site plugins from github: userName/repoName
     * It should find it on github
     */
    @Test
    @Network
    public void testInstallPluginWithGithub() throws Exception {
        assumeTrue(isDownloadServiceWorking("github.com", 443, "/"));
        testInstallRemovePlugin(null, "elasticsearch/kibana", "kibana", null, null);
    }

    // Private util methods

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

    private void runTestWithForbiddenName(String name) throws Exception {
        try {
            PluginManager.Install install = new PluginManager.Install(captureTerminal, null, name, null, new HttpDownloadHelper());
            Settings settings = settingsBuilder.build();
            Environment env = new Environment(settings);

            CliTool.ExitStatus status = install.execute(settings, env);
            assertThat(status, Matchers.is(CliTool.ExitStatus.OK));

            fail("this plugin name [" + name +
                    "] should not be allowed");
        } catch (ElasticsearchIllegalArgumentException e) {
            // We expect that error
        }
    }
}
