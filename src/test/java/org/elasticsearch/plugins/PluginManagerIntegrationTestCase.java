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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.http.client.HttpDownloadHelper;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.common.cli.CliTool.ExitStatus;
import static org.elasticsearch.common.cli.CliTool.ExitStatus.OK;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 * Base class for Plugin tool integration tests
 */
@ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE,
        numDataNodes = 0,
        numClientNodes = 0,
        transportClientRatio = 0.0)
public abstract class PluginManagerIntegrationTestCase extends ElasticsearchIntegrationTest {

    protected CliToolTestCase.CaptureOutputTerminal captureTerminal = new CliToolTestCase.CaptureOutputTerminal();
    protected Path homeDir;
    protected Path pluginsDir;
    protected Path binDir;
    protected Path configDir;
    protected ImmutableSettings.Builder settingsBuilder;

    protected static final String PLUGIN_NAME = "test-plugin";

    @Before
    public void init() throws Exception {
        homeDir = newTempDirPath();
        pluginsDir = homeDir.resolve("plugins");
//        Files.createDirectory(pluginsDir);
        binDir = homeDir.resolve("bin");
        Files.createDirectory(binDir);
        configDir = homeDir.resolve("config");
        Files.createDirectory(configDir);
        logger.info("  --> Tests will run in home dir [{}]", homeDir);
        settingsBuilder = ImmutableSettings.builder()
                .put("path.home", homeDir.toAbsolutePath());
    }

    protected void installPlugin(String localResourceName, String pluginName) throws Exception {
        installPlugin(localResourceName, pluginName, OK);
    }

    protected void installPlugin(String localResourceName, String pluginName, ExitStatus expectedStatus) throws Exception {
        HttpDownloadHelper downloadHelper;
        if (localResourceName == null) {
            downloadHelper = new HttpDownloadHelper();
        } else {
            downloadHelper = new ResourceDownloadHelper(localResourceName);
        }

        Settings settings = settingsBuilder.build();
        Environment env = new Environment(settings);

        captureTerminal.getTerminalOutput().clear();
        PluginManager.Install install = new PluginManager.Install(captureTerminal, localResourceName == null ? null : "http://fakeurl", pluginName, null, downloadHelper);
        ExitStatus status = install.execute(settings, env);
        assertThat(status, is(expectedStatus));

        if (expectedStatus == OK) {
            assertThat(captureTerminal.getTerminalOutput().isEmpty(), is(false));
            assertThat(captureTerminal.getTerminalOutput(), hasItem(containsString("installed successfully")));
        }
    }

    protected void removePlugin(String pluginName) throws Exception {
        Settings settings = settingsBuilder.build();
        Environment env = new Environment(settings);

        captureTerminal.getTerminalOutput().clear();
        PluginManager.Uninstall uninstall = new PluginManager.Uninstall(captureTerminal, pluginName);
        ExitStatus status = uninstall.execute(settings, env);
        assertThat(status, is(OK));

        assertThat(captureTerminal.getTerminalOutput().isEmpty(), is(false));
        assertThat(captureTerminal.getTerminalOutput(), hasItem(containsString("uninstalled successfully")));
    }

    protected void listPlugin(String pluginShortName, boolean expectedPresent) throws Exception {
        // We can check if plugin is correctly installed
        Settings settings = settingsBuilder.build();
        Environment env = new Environment(settings);
        captureTerminal.getTerminalOutput().clear();
        PluginManager.ListPlugins list = new PluginManager.ListPlugins(captureTerminal);
        ExitStatus status = list.execute(settings, env);
        assertThat(status, Matchers.is(OK));

        assertThat(captureTerminal.getTerminalOutput().isEmpty(), is(false));

        if (expectedPresent) {
            assertThat(captureTerminal.getTerminalOutput(), hasItem(containsString(pluginShortName)));
        } else {
            assertThat(captureTerminal.getTerminalOutput(), not(hasItem(containsString(pluginShortName))));
        }
    }

    protected void testInstallRemovePlugin(String localResourceName,
                                           String pluginFullName,
                                           String pluginShortName,
                                           PluginRulesTester afterInstall,
                                           PluginRulesTester afterUninstall) throws Exception {
        // We install the plugin
        installPlugin(localResourceName, pluginFullName, OK);
        if (afterInstall != null) {
            afterInstall.checkRules();
        }

        // We can check if plugin is correctly installed
        listPlugin(pluginShortName, true);

        // We can remove the plugin
        removePlugin(pluginFullName);
        if (afterUninstall != null) {
            afterUninstall.checkRules();
        }

        // We can check if plugin is correctly uninstalled
        listPlugin(pluginShortName, false);
    }

    /**
     * @param localResourceName
     * @param pluginFiles expected number of files or dirs in plugin dir, except _site (if null plugin must not exist)
     * @param siteFiles expected number of files in _site dir (if null plugin/_site must not exist)
     * @param configFiles expected number of files in config dir (if null config/plugin must not exist)
     * @param binFiles expected number of files in bin dir (if null bin/plugin must not exist)
     * @throws Exception
     */
    protected void testInstallRemovePlugin(String localResourceName,
                                         final Integer pluginFiles,
                                         final Integer siteFiles,
                                         final Integer configFiles,
                                         final Integer binFiles) throws Exception {
        testInstallRemovePlugin(localResourceName, PLUGIN_NAME, PLUGIN_NAME, new PluginRulesTester() {
            @Override
            public void checkRules() throws IOException {
                Path pluginDir = pluginsDir.resolve(PLUGIN_NAME);
                assertFileCount(pluginDir, pluginFiles == null ? null : pluginFiles + (siteFiles != null ? 1 : 0));
                Path siteDir = pluginDir.resolve("_site");
                assertFileCount(siteDir, siteFiles);
                Path pluginConfigDir = configDir.resolve(PLUGIN_NAME);
                assertFileCount(pluginConfigDir, configFiles);
                Path pluginBinDir = binDir.resolve(PLUGIN_NAME);
                assertFileCount(pluginBinDir, binFiles);
                if (Files.exists(pluginBinDir)) {
                    // If we have bin files, they should be set as executable
                    for (Path file : FileSystemUtils.files(pluginBinDir)) {
                        assertThat(Files.isExecutable(file), is(true));
                    }
                }
            }
        }, new PluginRulesTester() {
            @Override
            public void checkRules() {
                Path extractedDir = pluginsDir.resolve(PLUGIN_NAME);
                assertDirectoryDoesNotExist(extractedDir);
                // If we have installed a config, it should not be removed
                Path pluginConfigDir = configDir.resolve(PLUGIN_NAME);
                if (configFiles != null) {
                    assertDirectoryExists(pluginConfigDir);
                } else {
                    assertDirectoryDoesNotExist(pluginConfigDir);
                }
                Path pluginBinDir = binDir.resolve(PLUGIN_NAME);
                assertDirectoryDoesNotExist(pluginBinDir);
            }
        });
    }

    protected interface PluginRulesTester {
        public void checkRules() throws IOException;
    }

    public static class ResourceDownloadHelper extends HttpDownloadHelper {

        private final String resourceName;

        public ResourceDownloadHelper(String resourceName) {
            this.resourceName = resourceName;
        }

        @Override
        public boolean download(URL source, Path dest, @Nullable DownloadProgress progress, TimeValue timeout) throws Exception {
            progress.beginDownload();
            Streams.copy(getClass().getResourceAsStream(resourceName), Files.newOutputStream(dest));
            progress.endDownload();
            return true;
        }
    }

    public void assertFileCount(Path actual, Integer expected) throws IOException {
        if (expected == null) {
            assertFileDoesNotExist(actual);
        } else {
            assertFileExists(actual);
            Path[] files = FileSystemUtils.files(actual);
            if (files.length != expected) {
                logger.info("{} got {} files:", actual.toAbsolutePath(), files.length);
                for (Path file : files) {
                    logger.info("       - {}", file);
                }
            }
            assertThat(files, is(arrayWithSize(expected)));
        }
    }
}
