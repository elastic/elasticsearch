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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PosixPermissionsResetter;
import org.junit.After;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;

@LuceneTestCase.SuppressFileSystems("*")
public class InstallPluginCommandTests extends ESTestCase {

    private final Function<String, Path> temp;

    private final FileSystem fs;
    private final boolean isPosix;
    private final boolean isReal;
    private final String javaIoTmpdir;

    @SuppressForbidden(reason = "sets java.io.tmpdir")
    public InstallPluginCommandTests(FileSystem fs, Function<String, Path> temp) {
        this.fs = fs;
        this.temp = temp;
        this.isPosix = fs.supportedFileAttributeViews().contains("posix");
        this.isReal = fs == PathUtils.getDefaultFileSystem();
        PathUtilsForTesting.installMock(fs);
        javaIoTmpdir = System.getProperty("java.io.tmpdir");
        System.setProperty("java.io.tmpdir", temp.apply("tmpdir").toString());
    }

    @After
    @SuppressForbidden(reason = "resets java.io.tmpdir")
    public void tearDown() throws Exception {
        System.setProperty("java.io.tmpdir", javaIoTmpdir);
        PathUtilsForTesting.teardown();
        super.tearDown();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        class Parameter {
            private final FileSystem fileSystem;
            private final Function<String, Path> temp;

            public Parameter(FileSystem fileSystem, String root) {
                this(fileSystem, s -> {
                    try {
                        return Files.createTempDirectory(fileSystem.getPath(root), s);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            public Parameter(FileSystem fileSystem, Function<String, Path> temp) {
                this.fileSystem = fileSystem;
                this.temp = temp;
            }
        }
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(Jimfs.newFileSystem(Configuration.windows()), "c:\\"));
        parameters.add(new Parameter(Jimfs.newFileSystem(toPosix(Configuration.osX())), "/"));
        parameters.add(new Parameter(Jimfs.newFileSystem(toPosix(Configuration.unix())), "/"));
        parameters.add(new Parameter(PathUtils.getDefaultFileSystem(), LuceneTestCase::createTempDir ));
        return parameters.stream().map(p -> new Object[] { p.fileSystem, p.temp }).collect(Collectors.toList());
    }

    private static Configuration toPosix(Configuration configuration) {
        return configuration.toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build();
    }

    /** Creates a test environment with bin, config and plugins directories. */
    static Tuple<Path, Environment> createEnv(FileSystem fs, Function<String, Path> temp) throws IOException {
        Path home = temp.apply("install-plugin-command-tests");
        Files.createDirectories(home.resolve("bin"));
        Files.createFile(home.resolve("bin").resolve("elasticsearch"));
        Files.createDirectories(home.resolve("config"));
        Files.createFile(home.resolve("config").resolve("elasticsearch.yml"));
        Path plugins = Files.createDirectories(home.resolve("plugins"));
        assertTrue(Files.exists(plugins));
        Settings settings = Settings.builder()
            .put("path.home", home)
            .build();
        return Tuple.tuple(home, new Environment(settings));
    }

    static Path createPluginDir(Function<String, Path> temp) throws IOException {
        return temp.apply("pluginDir");
    }

    /** creates a fake jar file with empty class files */
    static void writeJar(Path jar, String... classes) throws IOException {
        try (ZipOutputStream stream = new ZipOutputStream(Files.newOutputStream(jar))) {
            for (String clazz : classes) {
                stream.putNextEntry(new ZipEntry(clazz + ".class")); // no package names, just support simple classes
            }
        }
    }

    static String writeZip(Path structure, String prefix) throws IOException {
        Path zip = createTempDir().resolve(structure.getFileName() + ".zip");
        try (ZipOutputStream stream = new ZipOutputStream(Files.newOutputStream(zip))) {
            Files.walkFileTree(structure, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String target = (prefix == null ? "" : prefix + "/") + structure.relativize(file).toString();
                    stream.putNextEntry(new ZipEntry(target));
                    Files.copy(file, stream);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        return zip.toUri().toURL().toString();
    }

    /** creates a plugin .zip and returns the url for testing */
    static String createPlugin(String name, Path structure) throws IOException {
        PluginTestUtil.writeProperties(structure,
            "description", "fake desc",
            "name", name,
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin");
        writeJar(structure.resolve("plugin.jar"), "FakePlugin");
        return writeZip(structure, "elasticsearch");
    }

    static MockTerminal installPlugin(String pluginUrl, Path home) throws Exception {
        return installPlugin(pluginUrl, home, false);
    }

    static MockTerminal installPlugin(String pluginUrl, Path home, boolean jarHellCheck) throws Exception {
        Map<String, String> settings = new HashMap<>();
        settings.put("path.home", home.toString());
        MockTerminal terminal = new MockTerminal();
        new InstallPluginCommand() {
            @Override
            void jarHellCheck(Path candidate, Path pluginsDir) throws Exception {
                if (jarHellCheck) {
                    super.jarHellCheck(candidate, pluginsDir);
                }
            }
        }.execute(terminal, pluginUrl, true, settings);
        return terminal;
    }

    void assertPlugin(String name, Path original, Environment env) throws IOException {
        Path got = env.pluginsFile().resolve(name);
        assertTrue("dir " + name + " exists", Files.exists(got));

        if (isPosix) {
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(got);
            assertThat(
                perms,
                containsInAnyOrder(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE,
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.GROUP_EXECUTE,
                    PosixFilePermission.OTHERS_READ,
                    PosixFilePermission.OTHERS_EXECUTE));
        }

        assertTrue("jar was copied", Files.exists(got.resolve("plugin.jar")));
        assertFalse("bin was not copied", Files.exists(got.resolve("bin")));
        assertFalse("config was not copied", Files.exists(got.resolve("config")));
        if (Files.exists(original.resolve("bin"))) {
            Path binDir = env.binFile().resolve(name);
            assertTrue("bin dir exists", Files.exists(binDir));
            assertTrue("bin is a dir", Files.isDirectory(binDir));
            PosixFileAttributes binAttributes = null;
            if (isPosix) {
                binAttributes = Files.readAttributes(env.binFile(), PosixFileAttributes.class);
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(binDir)) {
                for (Path file : stream) {
                    assertFalse("not a dir", Files.isDirectory(file));
                    if (isPosix) {
                        PosixFileAttributes attributes = Files.readAttributes(file, PosixFileAttributes.class);
                        assertEquals(InstallPluginCommand.DIR_AND_EXECUTABLE_PERMS, attributes.permissions());
                    }
                }
            }
        }
        if (Files.exists(original.resolve("config"))) {
            Path configDir = env.configFile().resolve(name);
            assertTrue("config dir exists", Files.exists(configDir));
            assertTrue("config is a dir", Files.isDirectory(configDir));

            if (isPosix) {
                Path configRoot = env.configFile();
                PosixFileAttributes configAttributes =
                    Files.getFileAttributeView(configRoot, PosixFileAttributeView.class).readAttributes();
                PosixFileAttributes attributes = Files.getFileAttributeView(configDir, PosixFileAttributeView.class).readAttributes();
                assertThat(attributes.owner(), equalTo(configAttributes.owner()));
                assertThat(attributes.group(), equalTo(configAttributes.group()));
            }

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(configDir)) {
                for (Path file : stream) {
                    assertFalse("not a dir", Files.isDirectory(file));
                }
            }
        }
        assertInstallCleaned(env);
    }

    void assertInstallCleaned(Environment env) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(env.pluginsFile())) {
            for (Path file : stream) {
                if (file.getFileName().toString().startsWith(".installing")) {
                    fail("Installation dir still exists, " + file);
                }
            }
        }
    }

    public void testSomethingWorks() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPlugin("fake", pluginDir);
        installPlugin(pluginZip, env.v1());
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testSpaceInUrl() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPlugin("fake", pluginDir);
        Path pluginZipWithSpaces = createTempFile("foo bar", ".zip");
        try (InputStream in = new URL(pluginZip).openStream()) {
            Files.copy(in, pluginZipWithSpaces, StandardCopyOption.REPLACE_EXISTING);
        }
        installPlugin(pluginZipWithSpaces.toUri().toURL().toString(), env.v1());
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testMalformedUrlNotMaven() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        // has two colons, so it appears similar to maven coordinates
        MalformedURLException e = expectThrows(MalformedURLException.class, () -> installPlugin("://host:1234", env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("no protocol"));
    }

    public void testUnknownPlugin() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        UserError e = expectThrows(UserError.class, () -> installPlugin("foo", env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("Unknown plugin foo"));
    }

    public void testPluginsDirMissing() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Files.delete(env.v2().pluginsFile());
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPlugin("fake", pluginDir);
        installPlugin(pluginZip, env.v1());
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testPluginsDirReadOnly() throws Exception {
        assumeTrue("posix and filesystem", isPosix && isReal);
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        try (PosixPermissionsResetter pluginsAttrs = new PosixPermissionsResetter(env.v2().pluginsFile())) {
            pluginsAttrs.setPermissions(new HashSet<>());
            String pluginZip = createPlugin("fake", pluginDir);
            IOException e = expectThrows(IOException.class, () -> installPlugin(pluginZip, env.v1()));
            assertTrue(e.getMessage(), e.getMessage().contains(env.v2().pluginsFile().toString()));
        }
        assertInstallCleaned(env.v2());
    }

    public void testBuiltinModule() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPlugin("lang-groovy", pluginDir);
        UserError e = expectThrows(UserError.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("is a system module"));
        assertInstallCleaned(env.v2());
    }

    public void testJarHell() throws Exception {
        // jar hell test needs a real filesystem
        assumeTrue("real filesystem", isReal);
        Tuple<Path, Environment> environment = createEnv(fs, temp);
        Path pluginDirectory = createPluginDir(temp);
        writeJar(pluginDirectory.resolve("other.jar"), "FakePlugin");
        String pluginZip = createPlugin("fake", pluginDirectory); // adds plugin.jar with FakePlugin
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> installPlugin(pluginZip, environment.v1(), true));
        assertTrue(e.getMessage(), e.getMessage().contains("jar hell"));
        assertInstallCleaned(environment.v2());
    }

    public void testIsolatedPlugins() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        // these both share the same FakePlugin class
        Path pluginDir1 = createPluginDir(temp);
        String pluginZip1 = createPlugin("fake1", pluginDir1);
        installPlugin(pluginZip1, env.v1());
        Path pluginDir2 = createPluginDir(temp);
        String pluginZip2 = createPlugin("fake2", pluginDir2);
        installPlugin(pluginZip2, env.v1());
        assertPlugin("fake1", pluginDir1, env.v2());
        assertPlugin("fake2", pluginDir2, env.v2());
    }

    public void testExistingPlugin() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPlugin("fake", pluginDir);
        installPlugin(pluginZip, env.v1());
        UserError e = expectThrows(UserError.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("already exists"));
        assertInstallCleaned(env.v2());
    }

    public void testBin() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path binDir = pluginDir.resolve("bin");
        Files.createDirectory(binDir);
        Files.createFile(binDir.resolve("somescript"));
        String pluginZip = createPlugin("fake", pluginDir);
        installPlugin(pluginZip, env.v1());
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testBinNotDir() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path binDir = pluginDir.resolve("bin");
        Files.createFile(binDir);
        String pluginZip = createPlugin("fake", pluginDir);
        UserError e = expectThrows(UserError.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("not a directory"));
        assertInstallCleaned(env.v2());
    }

    public void testBinContainsDir() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path dirInBinDir = pluginDir.resolve("bin").resolve("foo");
        Files.createDirectories(dirInBinDir);
        Files.createFile(dirInBinDir.resolve("somescript"));
        String pluginZip = createPlugin("fake", pluginDir);
        UserError e = expectThrows(UserError.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("Directories not allowed in bin dir for plugin"));
        assertInstallCleaned(env.v2());
    }

    public void testBinConflict() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path binDir = pluginDir.resolve("bin");
        Files.createDirectory(binDir);
        Files.createFile(binDir.resolve("somescript"));
        String pluginZip = createPlugin("elasticsearch", pluginDir);
        FileAlreadyExistsException e = expectThrows(FileAlreadyExistsException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains(env.v2().binFile().resolve("elasticsearch").toString()));
        assertInstallCleaned(env.v2());
    }

    public void testBinPermissions() throws Exception {
        assumeTrue("posix filesystem", isPosix);
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path binDir = pluginDir.resolve("bin");
        Files.createDirectory(binDir);
        Files.createFile(binDir.resolve("somescript"));
        String pluginZip = createPlugin("fake", pluginDir);
        try (PosixPermissionsResetter binAttrs = new PosixPermissionsResetter(env.v2().binFile())) {
            Set<PosixFilePermission> perms = binAttrs.getCopyPermissions();
            // make sure at least one execute perm is missing, so we know we forced it during installation
            perms.remove(PosixFilePermission.GROUP_EXECUTE);
            binAttrs.setPermissions(perms);
            installPlugin(pluginZip, env.v1());
            assertPlugin("fake", pluginDir, env.v2());
        }
    }

    public void testConfig() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path configDir = pluginDir.resolve("config");
        Files.createDirectory(configDir);
        Files.createFile(configDir.resolve("custom.yaml"));
        String pluginZip = createPlugin("fake", pluginDir);
        installPlugin(pluginZip, env.v1());
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testExistingConfig() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path envConfigDir = env.v2().configFile().resolve("fake");
        Files.createDirectories(envConfigDir);
        Files.write(envConfigDir.resolve("custom.yaml"), "existing config".getBytes(StandardCharsets.UTF_8));
        Path pluginDir = createPluginDir(temp);
        Path configDir = pluginDir.resolve("config");
        Files.createDirectory(configDir);
        Files.write(configDir.resolve("custom.yaml"), "new config".getBytes(StandardCharsets.UTF_8));
        Files.createFile(configDir.resolve("other.yaml"));
        String pluginZip = createPlugin("fake", pluginDir);
        installPlugin(pluginZip, env.v1());
        assertPlugin("fake", pluginDir, env.v2());
        List<String> configLines = Files.readAllLines(envConfigDir.resolve("custom.yaml"), StandardCharsets.UTF_8);
        assertEquals(1, configLines.size());
        assertEquals("existing config", configLines.get(0));
        assertTrue(Files.exists(envConfigDir.resolve("other.yaml")));
    }

    public void testConfigNotDir() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path configDir = pluginDir.resolve("config");
        Files.createFile(configDir);
        String pluginZip = createPlugin("fake", pluginDir);
        UserError e = expectThrows(UserError.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("not a directory"));
        assertInstallCleaned(env.v2());
    }

    public void testConfigContainsDir() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path dirInConfigDir = pluginDir.resolve("config").resolve("foo");
        Files.createDirectories(dirInConfigDir);
        Files.createFile(dirInConfigDir.resolve("myconfig.yml"));
        String pluginZip = createPlugin("fake", pluginDir);
        UserError e = expectThrows(UserError.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("Directories not allowed in config dir for plugin"));
        assertInstallCleaned(env.v2());
    }

    public void testConfigConflict() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path configDir = pluginDir.resolve("config");
        Files.createDirectory(configDir);
        Files.createFile(configDir.resolve("myconfig.yml"));
        String pluginZip = createPlugin("elasticsearch.yml", pluginDir);
        FileAlreadyExistsException e = expectThrows(FileAlreadyExistsException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains(env.v2().configFile().resolve("elasticsearch.yml").toString()));
        assertInstallCleaned(env.v2());
    }

    public void testMissingDescriptor() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Files.createFile(pluginDir.resolve("fake.yml"));
        String pluginZip = writeZip(pluginDir, "elasticsearch");
        NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("plugin-descriptor.properties"));
        assertInstallCleaned(env.v2());
    }

    public void testMissingDirectory() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Files.createFile(pluginDir.resolve(PluginInfo.ES_PLUGIN_PROPERTIES));
        String pluginZip = writeZip(pluginDir, null);
        UserError e = expectThrows(UserError.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("`elasticsearch` directory is missing in the plugin zip"));
        assertInstallCleaned(env.v2());
    }

    public void testZipRelativeOutsideEntryName() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path zip = createTempDir().resolve("broken.zip");
        try (ZipOutputStream stream = new ZipOutputStream(Files.newOutputStream(zip))) {
            stream.putNextEntry(new ZipEntry("elasticsearch/../blah"));
        }
        String pluginZip = zip.toUri().toURL().toString();
        IOException e = expectThrows(IOException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("resolving outside of plugin directory"));
    }

    public void testOfficialPluginsHelpSorted() throws Exception {
        MockTerminal terminal = new MockTerminal();
        new InstallPluginCommand().main(new String[] { "--help" }, terminal);
        try (BufferedReader reader = new BufferedReader(new StringReader(terminal.getOutput()))) {
            String line = reader.readLine();

            // first find the beginning of our list of official plugins
            while (line.endsWith("may be installed by name:") == false) {
                line = reader.readLine();
            }

            // now check each line compares greater than the last, until we reach an empty line
            String prev = reader.readLine();
            line = reader.readLine();
            while (line != null && line.trim().isEmpty() == false) {
                assertTrue(prev + " < " + line, prev.compareTo(line) < 0);
                prev = line;
                line = reader.readLine();
            }
        }
    }

    public void testOfficialPluginsIncludesXpack() throws Exception {
        MockTerminal terminal = new MockTerminal();
        new InstallPluginCommand().main(new String[] { "--help" }, terminal);
        assertTrue(terminal.getOutput(), terminal.getOutput().contains("x-pack"));
    }

    public void testInstallMisspelledOfficialPlugins() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        UserError e = expectThrows(UserError.class, () -> installPlugin("xpack", env.v1()));
        assertThat(e.getMessage(), containsString("Unknown plugin xpack, did you mean [x-pack]?"));

        e = expectThrows(UserError.class, () -> installPlugin("analysis-smartnc", env.v1()));
        assertThat(e.getMessage(), containsString("Unknown plugin analysis-smartnc, did you mean [analysis-smartcn]?"));

        e = expectThrows(UserError.class, () -> installPlugin("repository", env.v1()));
        assertThat(e.getMessage(), containsString("Unknown plugin repository, did you mean any of [repository-s3, repository-gcs]?"));

        e = expectThrows(UserError.class, () -> installPlugin("unknown_plugin", env.v1()));
        assertThat(e.getMessage(), containsString("Unknown plugin unknown_plugin"));
    }

    // TODO: test batch flag?
    // TODO: test checksum (need maven/official below)
    // TODO: test maven, official, and staging install...need tests with fixtures...
}
