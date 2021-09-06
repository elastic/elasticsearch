/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Proxy;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.plugins.ProxyMatcher.matchesProxy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@LuceneTestCase.SuppressFileSystems("*")
public class SyncPluginsCommandTests extends ESTestCase {

    private InstallPluginAction skipJarHellAction;
    private InstallPluginAction defaultAction;
    private Path pluginsFile;

    private final Function<String, Path> temp;
    private MockTerminal terminal;
    private Tuple<Path, Environment> env;
    private final String javaIoTmpdir;

    @SuppressForbidden(reason = "sets java.io.tmpdir")
    public SyncPluginsCommandTests(FileSystem fs, Function<String, Path> temp) {
        this.temp = temp;
        PathUtilsForTesting.installMock(fs);
        javaIoTmpdir = System.getProperty("java.io.tmpdir");
        System.setProperty("java.io.tmpdir", temp.apply("tmpdir").toString());
    }

    private InstallPluginAction installPluginAction;
    private RemovePluginAction removePluginAction;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        terminal = new MockTerminal();
        env = createEnv(temp);
        skipJarHellAction = new InstallPluginAction(terminal, null) {
            @Override
            void jarHellCheck(PluginInfo candidateInfo, Path candidate, Path pluginsDir, Path modulesDir) {
                // no jarhell check
            }
        };
        defaultAction = new InstallPluginAction(terminal, env.v2());

        installPluginAction = mock(InstallPluginAction.class);
        removePluginAction = mock(RemovePluginAction.class);

        pluginsFile = env.v2().configFile().resolve("elasticsearch-plugins.yml");
    }

    @Override
    @After
    @SuppressForbidden(reason = "resets java.io.tmpdir")
    public void tearDown() throws Exception {
        defaultAction.close();
        skipJarHellAction.close();
        System.setProperty("java.io.tmpdir", javaIoTmpdir);
        PathUtilsForTesting.teardown();
        super.tearDown();
    }

    /**
     * Generates all the parameters for the JUnit tests - in this case, filesystems to use.
     * @return junit parameters for {@link #SyncPluginsCommandTests(FileSystem, Function)}
     */
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        class Parameter {
            private final FileSystem fileSystem;
            private final Function<String, Path> temp;

            Parameter(FileSystem fileSystem, String root) {
                this(fileSystem, s -> {
                    try {
                        return Files.createTempDirectory(fileSystem.getPath(root), s);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            Parameter(FileSystem fileSystem, Function<String, Path> temp) {
                this.fileSystem = fileSystem;
                this.temp = temp;
            }
        }
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(Jimfs.newFileSystem(Configuration.windows()), "c:\\"));
        parameters.add(new Parameter(Jimfs.newFileSystem(toPosix(Configuration.osX())), "/"));
        parameters.add(new Parameter(Jimfs.newFileSystem(toPosix(Configuration.unix())), "/"));
        parameters.add(new Parameter(PathUtils.getDefaultFileSystem(), LuceneTestCase::createTempDir));
        return parameters.stream().map(p -> new Object[] { p.fileSystem, p.temp }).collect(Collectors.toList());
    }

    private static Configuration toPosix(Configuration configuration) {
        return configuration.toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build();
    }

    /** Creates a test environment with bin, config and plugins directories. */
    static Tuple<Path, Environment> createEnv(Function<String, Path> temp) throws IOException {
        Path home = temp.apply("install-plugin-command-tests");
        Files.createDirectories(home.resolve("bin"));
        Files.createFile(home.resolve("bin").resolve("elasticsearch"));
        Files.createDirectories(home.resolve("config"));
        Files.createFile(home.resolve("config").resolve("elasticsearch.yml"));
        Path plugins = Files.createDirectories(home.resolve("plugins"));
        assertTrue(Files.exists(plugins));
        Settings settings = Settings.builder().put("path.home", home).build();
        return Tuple.tuple(home, TestEnvironment.newEnvironment(settings));
    }

    /**
     * Check that the sync tool will run successfully with no plugins declared and no plugins installed.
     */
    public void testSync_withNoPlugins_succeeds() throws Exception {
        Files.writeString(pluginsFile, "plugins:\n");

        SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), false, removePluginAction, installPluginAction);

        verify(installPluginAction, never()).execute(any());
        verify(removePluginAction, never()).execute(any());
    }

    /**
     * Check that the sync tool will run successfully with an official plugin.
     */
    public void testSync_withPlugin_succeeds() throws Exception {
        StringJoiner yaml = new StringJoiner("\n", "", "\n");
        yaml.add("plugins:");
        yaml.add("  - id: analysis-icu");

        Files.writeString(pluginsFile, yaml.toString());

        SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), false, removePluginAction, installPluginAction);

        verify(removePluginAction, never()).execute(any());
        verify(installPluginAction).setProxy(Proxy.NO_PROXY);
        verify(installPluginAction).execute(List.of(new PluginDescriptor("analysis-icu")));
    }

    /**
     * Check that the sync tool will run successfully with an official plugin but with a URL specified.
     */
    public void testSync_withPluginAndProxy_succeeds() throws Exception {
        StringJoiner yaml = new StringJoiner("\n", "", "\n");
        yaml.add("plugins:");
        yaml.add("  - id: analysis-icu");
        yaml.add("proxy: example.com:8080");

        Files.writeString(pluginsFile, yaml.toString());

        SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), false, removePluginAction, installPluginAction);

        verify(removePluginAction, never()).execute(any());
        verify(installPluginAction).setProxy(argThat(matchesProxy(Proxy.Type.HTTP, "example.com", 8080)));
        verify(installPluginAction).execute(List.of(new PluginDescriptor("analysis-icu")));
    }

    /**
     * Check that the sync tool will print the corrects summary of changes with a plugin pending installation.
     */
    public void testSync_withDryRunAndPluginPending_printsCorrectSummary() throws Exception {
        StringJoiner yaml = new StringJoiner("\n", "", "\n");
        yaml.add("plugins:");
        yaml.add("  - id: analysis-icu");

        Files.writeString(pluginsFile, yaml.toString());

        SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), true, removePluginAction, installPluginAction);

        verify(removePluginAction, never()).execute(any());
        verify(installPluginAction, never()).execute(any());

        String expected = String.join("\n", "No plugins to remove.", "The following plugins need to be installed:", "", "    analysis-icu");

        assertThat(terminal.getOutput().trim(), equalTo(expected));
    }

    /**
     * Check that the sync tool will do nothing when a plugin is already installed.
     */
    public void testSync_withPluginAlreadyInstalled_succeeds() throws Exception {
        final String pluginId = "example-plugin";

        writePluginDescriptor(pluginId);

        final StringJoiner yaml = new StringJoiner("\n", "", "\n");
        yaml.add("plugins:");
        yaml.add("  - id: example-plugin");

        Files.writeString(pluginsFile, yaml.toString());

        final SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), false, removePluginAction, installPluginAction);

        verify(removePluginAction, never()).execute(any());
        verify(installPluginAction, never()).execute(any());
    }

    /**
     * Check that the sync tool will print the correct summary when a required plugin is already installed.
     */
    public void testSync_withDryRunAndPluginAlreadyInstalled_printsCorrectSummary() throws Exception {
        final String pluginId = "example-plugin";

        writePluginDescriptor(pluginId);

        final StringJoiner yaml = new StringJoiner("\n", "", "\n");
        yaml.add("plugins:");
        yaml.add("  - id: " + pluginId);

        Files.writeString(pluginsFile, yaml.toString());

        final SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), true, removePluginAction, installPluginAction);

        assertThat(terminal.getOutput().trim(), equalTo("No plugins to install or remove."));
    }

    /**
     * Check that the sync tool will run successfully when removing a plugin
     */
    public void testSync_withRemovePlugin_succeeds() throws Exception {
        final String pluginId = "example-plugin";

        writePluginDescriptor(pluginId);

        Files.writeString(pluginsFile, "plugins:");

        SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), false, removePluginAction, installPluginAction);

        verify(removePluginAction).execute(List.of(new PluginDescriptor(pluginId)));
        verify(installPluginAction, never()).execute(any());
    }

    /**
     * Check that the sync tool will print the correct summary in dry run mode for removing a plugin
     */
    public void testSync_withDryRunRemovePlugin_printsCorrectSummary() throws Exception {
        final String pluginId = "example-plugin";

        writePluginDescriptor(pluginId);

        Files.writeString(pluginsFile, "plugins:");

        SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), true, removePluginAction, installPluginAction);

        verify(removePluginAction, never()).execute(any());
        verify(installPluginAction, never()).execute(any());

        String expected = String.join(
            "\n",
            "The following plugins need to be removed:",
            "",
            "    " + pluginId,
            "",
            "No plugins to install."
        );

        assertThat(terminal.getOutput().trim(), equalTo(expected));
    }

    /**
     * Check that the sync tool will run successfully when adding and removing plugins
     */
    public void testSync_withPluginsToAddAndRemove_succeeds() throws Exception {
        // Remove 2 plugins...
        writePluginDescriptor("plugin-to-remove1");
        writePluginDescriptor("plugin-to-remove2");
        // ...And keep 1
        writePluginDescriptor("plugin-to-keep");

        final StringJoiner yaml = new StringJoiner("\n", "", "\n");
        yaml.add("plugins:");
        yaml.add("  - id: plugin-to-keep");
        yaml.add("  - id: plugin-to-add1");
        yaml.add("  - id: plugin-to-add2");
        Files.writeString(pluginsFile, yaml.toString());

        SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), false, removePluginAction, installPluginAction);

        verify(removePluginAction).execute(List.of(new PluginDescriptor("plugin-to-remove1"), new PluginDescriptor("plugin-to-remove2")));
        verify(installPluginAction).execute(List.of(new PluginDescriptor("plugin-to-add1"), new PluginDescriptor("plugin-to-add2")));
    }

    /**
     * Check that the sync tool will print the correct summary when adding and removing plugins
     */
    public void testSync_withDryRunPluginsToAddAndRemove_printsCorrectSummary() throws Exception {
        // Remove 2 plugins...
        writePluginDescriptor("plugin-to-remove1");
        writePluginDescriptor("plugin-to-remove2");
        // ...And keep 1
        writePluginDescriptor("plugin-to-keep");

        final StringJoiner yaml = new StringJoiner("\n", "", "\n");
        yaml.add("plugins:");
        yaml.add("  - id: plugin-to-keep");
        yaml.add("  - id: plugin-to-add1");
        yaml.add("  - id: plugin-to-add2");
        Files.writeString(pluginsFile, yaml.toString());

        SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), true, removePluginAction, installPluginAction);

        verify(removePluginAction, never()).execute(any());
        verify(installPluginAction, never()).execute(any());

        String expected = String.join(
            "\n",
            "The following plugins need to be removed:",
            "",
            "    plugin-to-remove1",
            "    plugin-to-remove2",
            "",
            "The following plugins need to be installed:",
            "",
            "    plugin-to-add1",
            "    plugin-to-add2"
        );

        assertThat(terminal.getOutput().trim(), equalTo(expected));
    }

    /**
     * Check that the sync tool will fail gracefully when the config file is missing.
     */
    public void testSync_withMissingConfig_fails() {
        final SyncPluginsCommand command = new SyncPluginsCommand();
        final UserException exception = expectThrows(UserException.class, () -> command.execute(terminal, env.v2(), false, null, null));

        assertThat(exception.getMessage(), startsWith("Plugin manifest file missing:"));
        assertThat(exception.exitCode, equalTo(ExitCodes.CONFIG));
    }

    /**
     * Check that the sync tool will fail gracefully when an invalid proxy is specified
     */
    public void testSync_withInvalidProxy_fails() throws Exception {
        final StringJoiner yaml = new StringJoiner("\n", "", "\n");
        yaml.add("plugins:");
        yaml.add("proxy: ftp://example.com");

        Files.writeString(pluginsFile, yaml.toString());

        final SyncPluginsCommand command = new SyncPluginsCommand();
        final UserException exception = expectThrows(UserException.class, () -> command.execute(terminal, env.v2(), false, null, null));

        assertThat(exception.getMessage(), startsWith("Malformed [proxy], expected [host:port] in"));
        assertThat(exception.exitCode, equalTo(ExitCodes.CONFIG));
    }

    /**
     * Check that the sync tool will fail gracefully when an invalid proxy is specified for a specific plugin
     */
    public void testSync_withInvalidPluginProxy_fails() throws Exception {
        final StringJoiner yaml = new StringJoiner("\n", "", "\n");
        yaml.add("plugins:");
        yaml.add("  - id: example-plugin");
        yaml.add("    proxy: ftp://example.com");

        Files.writeString(pluginsFile, yaml.toString());

        final SyncPluginsCommand command = new SyncPluginsCommand();
        final UserException exception = expectThrows(UserException.class, () -> command.execute(terminal, env.v2(), false, null, null));

        assertThat(exception.getMessage(), startsWith("Malformed [proxy] for plugin [example-plugin], expected [host:port] in"));
        assertThat(exception.exitCode, equalTo(ExitCodes.CONFIG));
    }

    /**
     * Check that the sync tool will run successfully with an unofficial plugin.
     */
    public void testSync_withUnofficialPlugin_succeeds() throws Exception {
        StringJoiner yaml = new StringJoiner("\n", "", "\n");
        yaml.add("plugins:");
        yaml.add("  - id: example-plugin");
        yaml.add("    url: https://example.com/example-plugin.zip");

        Files.writeString(pluginsFile, yaml.toString());

        SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), false, removePluginAction, installPluginAction);

        verify(removePluginAction, never()).execute(any());
        verify(installPluginAction).execute(List.of(new PluginDescriptor("example-plugin", "https://example.com/example-plugin.zip")));
    }

    /**
     * Check that the sync tool will run successfully with an unofficial plugin and a proxy.
     */
    public void testSync_withUnofficialPluginAndProxy_succeeds() throws Exception {
        StringJoiner yaml = new StringJoiner("\n", "", "\n");
        yaml.add("plugins:");
        yaml.add("  - id: example-plugin");
        yaml.add("    url: https://example.com/example-plugin.zip");
        yaml.add("    proxy: example-proxy.com:8080");

        Files.writeString(pluginsFile, yaml.toString());

        SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), false, removePluginAction, installPluginAction);

        verify(removePluginAction, never()).execute(any());
        verify(installPluginAction).execute(
            List.of(new PluginDescriptor("example-plugin", "https://example.com/example-plugin.zip", "example-proxy.com:8080"))
        );
    }

    private void writePluginDescriptor(String name) throws IOException {
        final Path pluginPath = env.v2().pluginsFile().resolve(name);

        final Properties props = new Properties();
        props.put("description", "fake desc");
        props.put("name", name);
        props.put("version", "1.0");
        props.put("elasticsearch.version", Version.CURRENT.toString());
        props.put("java.version", System.getProperty("java.specification.version"));
        props.put("classname", "FakePlugin");

        Path propertiesFile = pluginPath.resolve(PluginInfo.ES_PLUGIN_PROPERTIES);
        Files.createDirectories(propertiesFile.getParent());

        try (OutputStream out = Files.newOutputStream(propertiesFile)) {
            props.store(out, null);
        }
    }
}
