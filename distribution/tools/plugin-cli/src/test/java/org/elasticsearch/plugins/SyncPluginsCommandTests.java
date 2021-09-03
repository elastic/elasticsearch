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
    private Path pluginDir;

    private final boolean isPosix;
    private final boolean isReal;
    private final String javaIoTmpdir;

    @SuppressForbidden(reason = "sets java.io.tmpdir")
    public SyncPluginsCommandTests(FileSystem fs, Function<String, Path> temp) {
        this.temp = temp;
        this.isPosix = fs.supportedFileAttributeViews().contains("posix");
        this.isReal = fs == PathUtils.getDefaultFileSystem();
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
        pluginDir = createPluginDir(temp);
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

    static Path createPluginDir(Function<String, Path> temp) {
        return temp.apply("pluginDir");
    }
    //
    // /** creates a fake jar file with empty class files */
    // static void writeJar(Path jar, String... classes) throws IOException {
    // try (ZipOutputStream stream = new ZipOutputStream(Files.newOutputStream(jar))) {
    // for (String clazz : classes) {
    // stream.putNextEntry(new ZipEntry(clazz + ".class")); // no package names, just support simple classes
    // }
    // }
    // }
    //
    // static Path writeZip(Path structure, String prefix) throws IOException {
    // Path zip = createTempDir().resolve(structure.getFileName() + ".zip");
    // try (ZipOutputStream stream = new ZipOutputStream(Files.newOutputStream(zip))) {
    // forEachFileRecursively(structure, (file, attrs) -> {
    // String target = (prefix == null ? "" : prefix + "/") + structure.relativize(file);
    // stream.putNextEntry(new ZipEntry(target));
    // Files.copy(file, stream);
    // });
    // }
    // return zip;
    // }
    //
    // /** creates a plugin .zip and returns the url for testing */
    // static PluginDescriptor createPluginZip(String name, Path structure, String... additionalProps) throws IOException {
    // return createPlugin(name, structure, additionalProps);
    // }
    //
    // static void writePlugin(String name, Path structure, String... additionalProps) throws IOException {
    // String[] properties = Stream.concat(
    // Stream.of(
    // "description",
    // "fake desc",
    // "name",
    // name,
    // "version",
    // "1.0",
    // "elasticsearch.version",
    // Version.CURRENT.toString(),
    // "java.version",
    // System.getProperty("java.specification.version"),
    // "classname",
    // "FakePlugin"
    // ),
    // Arrays.stream(additionalProps)
    // ).toArray(String[]::new);
    // PluginTestUtil.writePluginProperties(structure, properties);
    // String className = name.substring(0, 1).toUpperCase(Locale.ENGLISH) + name.substring(1) + "Plugin";
    // writeJar(structure.resolve("plugin.jar"), className);
    // }
    //
    // static void writePluginSecurityPolicy(Path pluginDir, String... permissions) throws IOException {
    // StringBuilder securityPolicyContent = new StringBuilder("grant {\n ");
    // for (String permission : permissions) {
    // securityPolicyContent.append("permission java.lang.RuntimePermission \"");
    // securityPolicyContent.append(permission);
    // securityPolicyContent.append("\";");
    // }
    // securityPolicyContent.append("\n};\n");
    // Files.write(pluginDir.resolve("plugin-security.policy"), securityPolicyContent.toString().getBytes(StandardCharsets.UTF_8));
    // }
    //
    // static PluginDescriptor createPlugin(String name, Path structure, String... additionalProps) throws IOException {
    // writePlugin(name, structure, additionalProps);
    // return new PluginDescriptor(name, writeZip(structure, null).toUri().toURL().toString());
    // }
    //
    // void installPlugin(String id) throws Exception {
    // PluginDescriptor plugin = id == null ? null : new PluginDescriptor(id, id);
    // installPlugin(plugin, env.v1(), skipJarHellAction);
    // }
    //
    // void installPlugin(PluginDescriptor plugin) throws Exception {
    // installPlugin(plugin, env.v1(), skipJarHellAction);
    // }
    //
    // void installPlugins(final List<PluginDescriptor> plugins, final Path home) throws Exception {
    // installPlugins(plugins, home, skipJarHellAction);
    // }
    //
    // void installPlugin(PluginDescriptor plugin, Path home, InstallPluginAction action) throws Exception {
    // installPlugins(plugin == null ? List.of() : List.of(plugin), home, action);
    // }
    //
    // void installPlugins(final List<PluginDescriptor> plugins, final Path home, final InstallPluginAction action) throws Exception {
    // final Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", home).build());
    // action.setEnvironment(env);
    // action.execute(plugins);
    // }

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

        String expected = String.join(
            "\n",
            "No plugins to remove.",
            "The following plugins need to be installed:",
            "",
            "    analysis-icu"
        );

        assertThat(terminal.getOutput().trim(), equalTo(expected));
    }

    /**
     * Check that the sync tool will do nothing when a plugin is already installed.
     */
    public void testSync_withPluginAlreadyInstalled_succeeds() throws Exception {
        final String pluginId = "example-plugin";

        writePluginDescriptor(pluginId, env.v2().pluginsFile().resolve(pluginId));

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

        writePluginDescriptor(pluginId, env.v2().pluginsFile().resolve(pluginId));

        final StringJoiner yaml = new StringJoiner("\n", "", "\n");
        yaml.add("plugins:");
        yaml.add("  - id: example-plugin");

        Files.writeString(pluginsFile, yaml.toString());

        final SyncPluginsCommand command = new SyncPluginsCommand();
        command.execute(terminal, env.v2(), true, removePluginAction, installPluginAction);

        assertThat(terminal.getOutput().trim(), equalTo("No plugins to install or remove."));
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

    private static void writePluginDescriptor(String name, Path pluginPath) throws IOException {
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
