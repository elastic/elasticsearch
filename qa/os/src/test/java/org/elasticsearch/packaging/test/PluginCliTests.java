/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.test.PackagingTestCase.AwaitsFix;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.StringJoiner;

import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

@AwaitsFix(bugUrl = "Needs to be re-enabled")
public class PluginCliTests extends PackagingTestCase {

    private static final String EXAMPLE_PLUGIN_NAME = "custom-settings";
    private static final Path EXAMPLE_PLUGIN_ZIP;
    static {
        // re-read before each test so the plugin path can be manipulated within tests
        EXAMPLE_PLUGIN_ZIP = Paths.get(System.getProperty("tests.example-plugin", "/dummy/path"));
    }

    @Before
    public void filterDistros() {
        assumeFalse("no docker", distribution.isDocker());
    }

    @FunctionalInterface
    public interface PluginAction {
        void run(Shell.Result installResult) throws Exception;
    }

    private Shell.Result assertWithPlugin(Installation.Executable pluginTool, Path pluginZip, String pluginName, PluginAction action)
        throws Exception {
        Shell.Result installResult = pluginTool.run("install --batch \"" + pluginZip.toUri() + "\"");
        action.run(installResult);
        return pluginTool.run("remove " + pluginName);
    }

    private void assertWithExamplePlugin(PluginAction action) throws Exception {
        assertWithPlugin(installation.executables().pluginTool, EXAMPLE_PLUGIN_ZIP, EXAMPLE_PLUGIN_NAME, action);
    }

    public void test10Install() throws Exception {
        install();
    }

    public void test20SymlinkPluginsDir() throws Exception {
        Path pluginsDir = installation.plugins;
        Path stashedPluginsDir = createTempDir("stashed-plugins");

        Files.delete(stashedPluginsDir); // delete so we can replace it
        Files.move(pluginsDir, stashedPluginsDir);
        Path linkedPlugins = createTempDir("symlinked-plugins");
        Platforms.onLinux(() -> sh.run("chown elasticsearch:elasticsearch " + linkedPlugins.toString()));
        Files.createSymbolicLink(pluginsDir, linkedPlugins);
        assertWithExamplePlugin(installResult -> {
            assertWhileRunning(() -> {
                final String pluginsResponse = makeRequest(Request.Get("http://localhost:9200/_cat/plugins?h=component")).strip();
                assertThat(pluginsResponse, equalTo(EXAMPLE_PLUGIN_NAME));

                String settingsPath = "_cluster/settings?include_defaults&filter_path=defaults.custom.simple";
                final String settingsResponse = makeRequest(Request.Get("http://localhost:9200/" + settingsPath)).strip();
                assertThat(settingsResponse, equalTo("{\"defaults\":{\"custom\":{\"simple\":\"foo\"}}}"));
            });
        });

        Files.delete(pluginsDir);
        Files.move(stashedPluginsDir, pluginsDir);
    }

    public void test21CustomConfDir() throws Exception {
        withCustomConfig(confPath -> assertWithExamplePlugin(installResult -> {}));
    }

    public void test22PluginZipWithSpace() throws Exception {
        Path spacedDir = createTempDir("spaced dir");
        Path plugin = Files.copy(EXAMPLE_PLUGIN_ZIP, spacedDir.resolve(EXAMPLE_PLUGIN_ZIP.getFileName()));
        assertWithPlugin(installation.executables().pluginTool, plugin, EXAMPLE_PLUGIN_NAME, installResult -> {});
    }

    public void test23ElasticsearchWithSpace() throws Exception {
        assumeTrue(distribution.isArchive());

        Path spacedDir = createTempDir("spaced dir");
        Path elasticsearch = spacedDir.resolve("elasticsearch");
        Files.move(installation.home, elasticsearch);
        Installation spacedInstallation = Installation.ofArchive(sh, distribution, elasticsearch);

        assertWithPlugin(spacedInstallation.executables().pluginTool, EXAMPLE_PLUGIN_ZIP, EXAMPLE_PLUGIN_NAME, installResult -> {});

        Files.move(elasticsearch, installation.home);
    }

    public void test24JavaOpts() throws Exception {
        sh.getEnv().put("ES_JAVA_OPTS", "-XX:+PrintFlagsFinal");
        assertWithExamplePlugin(installResult -> assertThat(installResult.stdout, containsString("MaxHeapSize")));
    }

    public void test25Umask() throws Exception {
        sh.setUmask("0077");
        assertWithExamplePlugin(installResult -> {});
    }

    /**
     * Check that the `install` subcommand cannot be used if a plugins config file exists.
     */
    public void test101InstallFailsIfConfigFilePresent() throws IOException {
        Files.writeString(installation.config.resolve("elasticsearch-plugins.yml"), "");

        Shell.Result result = installation.executables().pluginTool.runIgnoreExitCode("install", "analysis-icu");
        assertThat(result.isSuccess(), is(false));
        assertThat(result.stderr, matchesRegex("Plugins config \\[[^+]] exists, please use \\[elasticsearch-plugin sync] instead"));
    }

    /**
     * Check that the `remove` subcommand cannot be used if a plugins config file exists.
     */
    public void test102RemoveFailsIfConfigFilePresent() throws IOException {
        Files.writeString(installation.config.resolve("elasticsearch-plugins.yml"), "");

        Shell.Result result = installation.executables().pluginTool.runIgnoreExitCode("remove", "analysis-icu");
        assertThat(result.isSuccess(), is(false));
        assertThat(result.stderr, matchesRegex("Plugins config \\[[^+]] exists, please use \\[elasticsearch-plugin sync] instead"));
    }

    /**
     * Check that when a valid plugins config file exists, Elasticsearch starts
     * up successfully.
     */
    public void test103StartsSuccessfullyWhenPluginsConfigExists() throws Exception {
        try {
            StringJoiner yaml = new StringJoiner("\n", "", "\n");
            yaml.add("plugins:");
            yaml.add("  - id: fake");
            yaml.add("    location: file://" + EXAMPLE_PLUGIN_ZIP);

            Files.writeString(installation.config("elasticsearch-plugins.yml"), yaml.toString());
            assertWhileRunning(() -> {
                Shell.Result result = installation.executables().pluginTool.run("list");
                assertThat(result.stdout.trim(), equalTo("fake"));
            });
        } finally {
            FileUtils.rm(installation.config("elasticsearch-plugins.yml"));
            FileUtils.rm(installation.plugins.resolve(EXAMPLE_PLUGIN_NAME));
        }
    }

    /**
     * Check that when an invalid plugins config file exists, Elasticsearch does not start up.
     */
    public void test104FailsToStartWhenPluginsConfigIsInvalid() throws Exception {
        try {
            Files.writeString(installation.config("elasticsearch-plugins.yml"), "invalid_key:\n");
            Shell.Result result = runElasticsearchStartCommand(null, false, true);
            assertThat(result.isSuccess(), equalTo(false));
            assertThat(result.stderr, containsString("Cannot parse plugins config file"));
        } finally {
            FileUtils.rm(installation.config("elasticsearch-plugins.yml"));
        }
    }
}
