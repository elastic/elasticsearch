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
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PosixPermissionsResetter;

import java.io.IOException;
import java.io.InputStream;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;

@LuceneTestCase.SuppressFileSystems("*")
public class InstallPluginCommandTests extends ESTestCase {

    private final Function<String, Path> temp;

    private final FileSystem fs;
    private final boolean isPosix;
    private final boolean isReal;

    public InstallPluginCommandTests(FileSystem fs, Function<String, Path> temp) {
        this.fs = fs;
        this.temp = temp;
        this.isPosix = fs.supportedFileAttributeViews().contains("posix");
        this.isReal = fs == PathUtils.getDefaultFileSystem();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        class Parameter {
            private final FileSystem fileSystem;
            private final Function<String, Path> temp;

            public Parameter(FileSystem fileSystem, Supplier<String> root) {
                this(fileSystem, root, s -> {
                    try {
                        return Files.createTempDirectory(fileSystem.getPath(root.get()), s);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            public Parameter(FileSystem fileSystem, Supplier<String> root, Function<String, Path> temp) {
                this.fileSystem = fileSystem;
                this.temp = temp;
            }
        }
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(Jimfs.newFileSystem(Configuration.windows()), () -> "c:\\"));
        parameters.add(new Parameter(Jimfs.newFileSystem(toPosix(Configuration.osX())), () -> "/"));
        parameters.add(new Parameter(Jimfs.newFileSystem(toPosix(Configuration.unix())), () -> "/"));
        parameters.add(new Parameter(PathUtils.getDefaultFileSystem(), () -> createTempDir().toString(), LuceneTestCase::createTempDir ));
        return parameters.stream().map(p -> new Object[] { p.fileSystem, p.temp }).collect(Collectors.toList());
    }

    private static Configuration toPosix(Configuration configuration) {
        return configuration.toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build();
    }

    /** Creates a test environment with bin, config and plugins directories. */
    static Environment createEnv(FileSystem fs, Function<String, Path> temp) throws IOException {
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
        return new Environment(fs, settings);
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

    static MockTerminal installPlugin(String pluginUrl, Environment env) throws Exception {
        return installPlugin(pluginUrl, env, false);
    }

    static MockTerminal installPlugin(String pluginUrl, Environment env, boolean jarHellCheck) throws Exception {
        MockTerminal terminal = new MockTerminal();
        new InstallPluginCommand(env) {
            @Override
            void jarHellCheck(Path candidate, Path pluginsDir, boolean isolated) throws Exception {
                if (jarHellCheck) {
                    super.jarHellCheck(candidate, pluginsDir, isolated);
                }
            }
        }.execute(terminal, pluginUrl, true);
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
                        Set<PosixFilePermission> expectedPermissions = new HashSet<>(binAttributes.permissions());
                        expectedPermissions.add(PosixFilePermission.OWNER_EXECUTE);
                        expectedPermissions.add(PosixFilePermission.GROUP_EXECUTE);
                        expectedPermissions.add(PosixFilePermission.OTHERS_EXECUTE);
                        assertEquals(expectedPermissions, attributes.permissions());
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
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPlugin("fake", pluginDir);
        installPlugin(pluginZip, env);
        assertPlugin("fake", pluginDir, env);
    }

    public void testSpaceInUrl() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPlugin("fake", pluginDir);
        Path pluginZipWithSpaces = createTempFile("foo bar", ".zip");
        try (InputStream in = new URL(pluginZip).openStream()) {
            Files.copy(in, pluginZipWithSpaces, StandardCopyOption.REPLACE_EXISTING);
        }
        installPlugin(pluginZipWithSpaces.toUri().toURL().toString(), env);
        assertPlugin("fake", pluginDir, env);
    }

    public void testMalformedUrlNotMaven() throws Exception {
        Environment env = createEnv(fs, temp);
        // has two colons, so it appears similar to maven coordinates
        MalformedURLException e = expectThrows(MalformedURLException.class, () -> {
            installPlugin("://host:1234", env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("no protocol"));
    }

    public void testPluginsDirMissing() throws Exception {
        Environment env = createEnv(fs, temp);
        Files.delete(env.pluginsFile());
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPlugin("fake", pluginDir);
        installPlugin(pluginZip, env);
        assertPlugin("fake", pluginDir, env);
    }

    public void testPluginsDirReadOnly() throws Exception {
        assumeTrue("posix and filesystem", isPosix && isReal);
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        try (PosixPermissionsResetter pluginsAttrs = new PosixPermissionsResetter(env.pluginsFile())) {
            pluginsAttrs.setPermissions(new HashSet<>());
            String pluginZip = createPlugin("fake", pluginDir);
            IOException e = expectThrows(IOException.class, () -> {
                installPlugin(pluginZip, env);
            });
            assertTrue(e.getMessage(), e.getMessage().contains(env.pluginsFile().toString()));
        }
        assertInstallCleaned(env);
    }

    public void testBuiltinModule() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPlugin("lang-groovy", pluginDir);
        UserError e = expectThrows(UserError.class, () -> {
            installPlugin(pluginZip, env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("is a system module"));
        assertInstallCleaned(env);
    }

    public void testJarHell() throws Exception {
        // jar hell test needs a real filesystem
        assumeTrue("real filesystem", isReal);
        Environment environment = createEnv(fs, temp);
        Path pluginDirectory = createPluginDir(temp);
        writeJar(pluginDirectory.resolve("other.jar"), "FakePlugin");
        String pluginZip = createPlugin("fake", pluginDirectory); // adds plugin.jar with FakePlugin
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            installPlugin(pluginZip, environment, true);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("jar hell"));
        assertInstallCleaned(environment);
    }

    public void testIsolatedPlugins() throws Exception {
        Environment env = createEnv(fs, temp);
        // these both share the same FakePlugin class
        Path pluginDir1 = createPluginDir(temp);
        String pluginZip1 = createPlugin("fake1", pluginDir1);
        installPlugin(pluginZip1, env);
        Path pluginDir2 = createPluginDir(temp);
        String pluginZip2 = createPlugin("fake2", pluginDir2);
        installPlugin(pluginZip2, env);
        assertPlugin("fake1", pluginDir1, env);
        assertPlugin("fake2", pluginDir2, env);
    }

    public void testPurgatoryJarHell() throws Exception {
        assumeTrue("real filesystem", isReal);
        Environment environment = createEnv(fs, temp);
        Path pluginDir1 = createPluginDir(temp);
        PluginTestUtil.writeProperties(pluginDir1,
            "description", "fake desc",
            "name", "fake1",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin",
            "isolated", "false");
        writeJar(pluginDir1.resolve("plugin.jar"), "FakePlugin");
        String pluginZip1 = writeZip(pluginDir1, "elasticsearch");
        installPlugin(pluginZip1, environment);

        Path pluginDir2 = createPluginDir(temp);
        PluginTestUtil.writeProperties(pluginDir2,
            "description", "fake desc",
            "name", "fake2",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin",
            "isolated", "false");
        writeJar(pluginDir2.resolve("plugin.jar"), "FakePlugin");
        String pluginZip2 = writeZip(pluginDir2, "elasticsearch");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            installPlugin(pluginZip2, environment, true);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("jar hell"));
        assertInstallCleaned(environment);
    }

    public void testExistingPlugin() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPlugin("fake", pluginDir);
        installPlugin(pluginZip, env);
        UserError e = expectThrows(UserError.class, () -> {
            installPlugin(pluginZip, env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("already exists"));
        assertInstallCleaned(env);
    }

    public void testBin() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path binDir = pluginDir.resolve("bin");
        Files.createDirectory(binDir);
        Files.createFile(binDir.resolve("somescript"));
        String pluginZip = createPlugin("fake", pluginDir);
        installPlugin(pluginZip, env);
        assertPlugin("fake", pluginDir, env);
    }

    public void testBinNotDir() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path binDir = pluginDir.resolve("bin");
        Files.createFile(binDir);
        String pluginZip = createPlugin("fake", pluginDir);
        UserError e = expectThrows(UserError.class, () -> {
            installPlugin(pluginZip, env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("not a directory"));
        assertInstallCleaned(env);
    }

    public void testBinContainsDir() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path dirInBinDir = pluginDir.resolve("bin").resolve("foo");
        Files.createDirectories(dirInBinDir);
        Files.createFile(dirInBinDir.resolve("somescript"));
        String pluginZip = createPlugin("fake", pluginDir);
        UserError e = expectThrows(UserError.class, () -> {
            installPlugin(pluginZip, env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Directories not allowed in bin dir for plugin"));
        assertInstallCleaned(env);
    }

    public void testBinConflict() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path binDir = pluginDir.resolve("bin");
        Files.createDirectory(binDir);
        Files.createFile(binDir.resolve("somescript"));
        String pluginZip = createPlugin("elasticsearch", pluginDir);
        FileAlreadyExistsException e = expectThrows(FileAlreadyExistsException.class, () -> {
            installPlugin(pluginZip, env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains(env.binFile().resolve("elasticsearch").toString()));
        assertInstallCleaned(env);
    }

    public void testBinPermissions() throws Exception {
        assumeTrue("posix filesystem", isPosix);
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path binDir = pluginDir.resolve("bin");
        Files.createDirectory(binDir);
        Files.createFile(binDir.resolve("somescript"));
        String pluginZip = createPlugin("fake", pluginDir);
        try (PosixPermissionsResetter binAttrs = new PosixPermissionsResetter(env.binFile())) {
            Set<PosixFilePermission> perms = binAttrs.getCopyPermissions();
            // make sure at least one execute perm is missing, so we know we forced it during installation
            perms.remove(PosixFilePermission.GROUP_EXECUTE);
            binAttrs.setPermissions(perms);
            installPlugin(pluginZip, env);
            assertPlugin("fake", pluginDir, env);
        }
    }

    public void testConfig() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path configDir = pluginDir.resolve("config");
        Files.createDirectory(configDir);
        Files.createFile(configDir.resolve("custom.yaml"));
        String pluginZip = createPlugin("fake", pluginDir);
        installPlugin(pluginZip, env);
        assertPlugin("fake", pluginDir, env);
    }

    public void testExistingConfig() throws Exception {
        Environment env = createEnv(fs, temp);
        Path envConfigDir = env.configFile().resolve("fake");
        Files.createDirectories(envConfigDir);
        Files.write(envConfigDir.resolve("custom.yaml"), "existing config".getBytes(StandardCharsets.UTF_8));
        Path pluginDir = createPluginDir(temp);
        Path configDir = pluginDir.resolve("config");
        Files.createDirectory(configDir);
        Files.write(configDir.resolve("custom.yaml"), "new config".getBytes(StandardCharsets.UTF_8));
        Files.createFile(configDir.resolve("other.yaml"));
        String pluginZip = createPlugin("fake", pluginDir);
        installPlugin(pluginZip, env);
        assertPlugin("fake", pluginDir, env);
        List<String> configLines = Files.readAllLines(envConfigDir.resolve("custom.yaml"), StandardCharsets.UTF_8);
        assertEquals(1, configLines.size());
        assertEquals("existing config", configLines.get(0));
        assertTrue(Files.exists(envConfigDir.resolve("other.yaml")));
    }

    public void testConfigNotDir() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path configDir = pluginDir.resolve("config");
        Files.createFile(configDir);
        String pluginZip = createPlugin("fake", pluginDir);
        UserError e = expectThrows(UserError.class, () -> {
            installPlugin(pluginZip, env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("not a directory"));
        assertInstallCleaned(env);
    }

    public void testConfigContainsDir() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path dirInConfigDir = pluginDir.resolve("config").resolve("foo");
        Files.createDirectories(dirInConfigDir);
        Files.createFile(dirInConfigDir.resolve("myconfig.yml"));
        String pluginZip = createPlugin("fake", pluginDir);
        UserError e = expectThrows(UserError.class, () -> {
            installPlugin(pluginZip, env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Directories not allowed in config dir for plugin"));
        assertInstallCleaned(env);
    }

    public void testConfigConflict() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path configDir = pluginDir.resolve("config");
        Files.createDirectory(configDir);
        Files.createFile(configDir.resolve("myconfig.yml"));
        String pluginZip = createPlugin("elasticsearch.yml", pluginDir);
        FileAlreadyExistsException e = expectThrows(FileAlreadyExistsException.class, () -> {
            installPlugin(pluginZip, env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains(env.configFile().resolve("elasticsearch.yml").toString()));
        assertInstallCleaned(env);
    }

    public void testMissingDescriptor() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Files.createFile(pluginDir.resolve("fake.yml"));
        String pluginZip = writeZip(pluginDir, "elasticsearch");
        NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> {
            installPlugin(pluginZip, env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("plugin-descriptor.properties"));
        assertInstallCleaned(env);
    }

    public void testMissingDirectory() throws Exception {
        Environment env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Files.createFile(pluginDir.resolve(PluginInfo.ES_PLUGIN_PROPERTIES));
        String pluginZip = writeZip(pluginDir, null);
        UserError e = expectThrows(UserError.class, () -> {
            installPlugin(pluginZip, env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("`elasticsearch` directory is missing in the plugin zip"));
        assertInstallCleaned(env);
    }

    public void testZipRelativeOutsideEntryName() throws Exception {
        Environment env = createEnv(fs, temp);
        Path zip = createTempDir().resolve("broken.zip");
        try (ZipOutputStream stream = new ZipOutputStream(Files.newOutputStream(zip))) {
            stream.putNextEntry(new ZipEntry("elasticsearch/../blah"));
        }
        String pluginZip = zip.toUri().toURL().toString();
        IOException e = expectThrows(IOException.class, () -> {
            installPlugin(pluginZip, env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("resolving outside of plugin directory"));
    }

    // TODO: test batch flag?
    // TODO: test checksum (need maven/official below)
    // TODO: test maven, official, and staging install...need tests with fixtures...
}
