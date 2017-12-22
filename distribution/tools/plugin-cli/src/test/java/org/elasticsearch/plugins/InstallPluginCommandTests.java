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
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PosixPermissionsResetter;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URI;
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
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.not;

@LuceneTestCase.SuppressFileSystems("*")
public class InstallPluginCommandTests extends ESTestCase {

    private InstallPluginCommand skipJarHellCommand;
    private InstallPluginCommand defaultCommand;

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

    @Before
    public void setUp() throws Exception {
        super.setUp();
        skipJarHellCommand = new InstallPluginCommand() {
            @Override
            void jarHellCheck(Path candidate, Path pluginsDir) throws Exception {
                // no jarhell check
            }
        };
        defaultCommand = new InstallPluginCommand();
    }

    @After
    @SuppressForbidden(reason = "resets java.io.tmpdir")
    public void tearDown() throws Exception {
        defaultCommand.close();
        skipJarHellCommand.close();
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
        return Tuple.tuple(home, TestEnvironment.newEnvironment(settings));
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

    static Path writeZip(Path structure, String prefix) throws IOException {
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
        return zip;
    }

    /** creates a plugin .zip and returns the url for testing */
    static String createPluginUrl(String name, Path structure, String... additionalProps) throws IOException {
        return createPlugin(name, structure, false, additionalProps).toUri().toURL().toString();
    }

    static Path createPlugin(String name, Path structure, boolean createSecurityPolicyFile, String... additionalProps) throws IOException {
        String[] properties = Stream.concat(Stream.of(
            "description", "fake desc",
            "name", name,
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin"
        ), Arrays.stream(additionalProps)).toArray(String[]::new);
        PluginTestUtil.writeProperties(structure, properties);
        if (createSecurityPolicyFile) {
            String securityPolicyContent = "grant {\n  permission java.lang.RuntimePermission \"setFactory\";\n};\n";
            Files.write(structure.resolve("plugin-security.policy"), securityPolicyContent.getBytes(StandardCharsets.UTF_8));
        }
        writeJar(structure.resolve("plugin.jar"), "FakePlugin");
        return writeZip(structure, "elasticsearch");
    }

    MockTerminal installPlugin(String pluginUrl, Path home) throws Exception {
        return installPlugin(pluginUrl, home, skipJarHellCommand);
    }

    MockTerminal installPlugin(String pluginUrl, Path home, InstallPluginCommand command) throws Exception {
        Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", home).build());
        MockTerminal terminal = new MockTerminal();
        command.execute(terminal, pluginUrl, true, env);
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
                        assertEquals(InstallPluginCommand.BIN_FILES_PERMS, attributes.permissions());
                    }
                }
            }
        }
        if (Files.exists(original.resolve("config"))) {
            Path configDir = env.configFile().resolve(name);
            assertTrue("config dir exists", Files.exists(configDir));
            assertTrue("config is a dir", Files.isDirectory(configDir));

            UserPrincipal user = null;
            GroupPrincipal group = null;

            if (isPosix) {
                PosixFileAttributes configAttributes =
                        Files.getFileAttributeView(env.configFile(), PosixFileAttributeView.class).readAttributes();
                user = configAttributes.owner();
                group = configAttributes.group();

                PosixFileAttributes attributes = Files.getFileAttributeView(configDir, PosixFileAttributeView.class).readAttributes();
                assertThat(attributes.owner(), equalTo(user));
                assertThat(attributes.group(), equalTo(group));
            }

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(configDir)) {
                for (Path file : stream) {
                    assertFalse("not a dir", Files.isDirectory(file));

                    if (isPosix) {
                        PosixFileAttributes attributes = Files.readAttributes(file, PosixFileAttributes.class);
                        if (user != null) {
                            assertThat(attributes.owner(), equalTo(user));
                        }
                        if (group != null) {
                            assertThat(attributes.group(), equalTo(group));
                        }
                    }
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

    public void testMissingPluginId() throws IOException {
        final Tuple<Path, Environment> env = createEnv(fs, temp);
        final UserException e = expectThrows(UserException.class, () -> installPlugin(null, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("plugin id is required"));
    }

    public void testSomethingWorks() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPluginUrl("fake", pluginDir);
        installPlugin(pluginZip, env.v1());
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testInstallFailsIfPreviouslyRemovedPluginFailed() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPluginUrl("fake", pluginDir);
        final Path removing = env.v2().pluginsFile().resolve(".removing-failed");
        Files.createDirectory(removing);
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> installPlugin(pluginZip, env.v1()));
        final String expected = String.format(
                Locale.ROOT,
                "found file [%s] from a failed attempt to remove the plugin [failed]; execute [elasticsearch-plugin remove failed]",
                removing);
        assertThat(e, hasToString(containsString(expected)));
    }

    public void testSpaceInUrl() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPluginUrl("fake", pluginDir);
        Path pluginZipWithSpaces = createTempFile("foo bar", ".zip");
        try (InputStream in = FileSystemUtils.openFileURLStream(new URL(pluginZip))) {
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
        UserException e = expectThrows(UserException.class, () -> installPlugin("foo", env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("Unknown plugin foo"));
    }

    public void testPluginsDirReadOnly() throws Exception {
        assumeTrue("posix and filesystem", isPosix && isReal);
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        try (PosixPermissionsResetter pluginsAttrs = new PosixPermissionsResetter(env.v2().pluginsFile())) {
            pluginsAttrs.setPermissions(new HashSet<>());
            String pluginZip = createPluginUrl("fake", pluginDir);
            IOException e = expectThrows(IOException.class, () -> installPlugin(pluginZip, env.v1()));
            assertTrue(e.getMessage(), e.getMessage().contains(env.v2().pluginsFile().toString()));
        }
        assertInstallCleaned(env.v2());
    }

    public void testBuiltinModule() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPluginUrl("lang-painless", pluginDir);
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("is a system module"));
        assertInstallCleaned(env.v2());
    }

    public void testJarHell() throws Exception {
        // jar hell test needs a real filesystem
        assumeTrue("real filesystem", isReal);
        Tuple<Path, Environment> environment = createEnv(fs, temp);
        Path pluginDirectory = createPluginDir(temp);
        writeJar(pluginDirectory.resolve("other.jar"), "FakePlugin");
        String pluginZip = createPluginUrl("fake", pluginDirectory); // adds plugin.jar with FakePlugin
        IllegalStateException e = expectThrows(IllegalStateException.class,
            () -> installPlugin(pluginZip, environment.v1(), defaultCommand));
        assertTrue(e.getMessage(), e.getMessage().contains("jar hell"));
        assertInstallCleaned(environment.v2());
    }

    public void testIsolatedPlugins() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        // these both share the same FakePlugin class
        Path pluginDir1 = createPluginDir(temp);
        String pluginZip1 = createPluginUrl("fake1", pluginDir1);
        installPlugin(pluginZip1, env.v1());
        Path pluginDir2 = createPluginDir(temp);
        String pluginZip2 = createPluginUrl("fake2", pluginDir2);
        installPlugin(pluginZip2, env.v1());
        assertPlugin("fake1", pluginDir1, env.v2());
        assertPlugin("fake2", pluginDir2, env.v2());
    }

    public void testExistingPlugin() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPluginUrl("fake", pluginDir);
        installPlugin(pluginZip, env.v1());
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("already exists"));
        assertInstallCleaned(env.v2());
    }

    public void testBin() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path binDir = pluginDir.resolve("bin");
        Files.createDirectory(binDir);
        Files.createFile(binDir.resolve("somescript"));
        String pluginZip = createPluginUrl("fake", pluginDir);
        installPlugin(pluginZip, env.v1());
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testBinNotDir() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path binDir = pluginDir.resolve("bin");
        Files.createFile(binDir);
        String pluginZip = createPluginUrl("fake", pluginDir);
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("not a directory"));
        assertInstallCleaned(env.v2());
    }

    public void testBinContainsDir() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path dirInBinDir = pluginDir.resolve("bin").resolve("foo");
        Files.createDirectories(dirInBinDir);
        Files.createFile(dirInBinDir.resolve("somescript"));
        String pluginZip = createPluginUrl("fake", pluginDir);
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("Directories not allowed in bin dir for plugin"));
        assertInstallCleaned(env.v2());
    }

    public void testBinConflict() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path binDir = pluginDir.resolve("bin");
        Files.createDirectory(binDir);
        Files.createFile(binDir.resolve("somescript"));
        String pluginZip = createPluginUrl("elasticsearch", pluginDir);
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
        String pluginZip = createPluginUrl("fake", pluginDir);
        try (PosixPermissionsResetter binAttrs = new PosixPermissionsResetter(env.v2().binFile())) {
            Set<PosixFilePermission> perms = binAttrs.getCopyPermissions();
            // make sure at least one execute perm is missing, so we know we forced it during installation
            perms.remove(PosixFilePermission.GROUP_EXECUTE);
            binAttrs.setPermissions(perms);
            installPlugin(pluginZip, env.v1());
            assertPlugin("fake", pluginDir, env.v2());
        }
    }

    public void testPluginPermissions() throws Exception {
        assumeTrue("posix filesystem", isPosix);

        final Tuple<Path, Environment> env = createEnv(fs, temp);
        final Path pluginDir = createPluginDir(temp);
        final Path resourcesDir = pluginDir.resolve("resources");
        final Path platformDir = pluginDir.resolve("platform");
        final Path platformNameDir = platformDir.resolve("linux-x86_64");
        final Path platformBinDir = platformNameDir.resolve("bin");
        Files.createDirectories(platformBinDir);

        Files.createFile(pluginDir.resolve("fake-" + Version.CURRENT.toString() + ".jar"));
        Files.createFile(platformBinDir.resolve("fake_executable"));
        Files.createDirectory(resourcesDir);
        Files.createFile(resourcesDir.resolve("resource"));

        final String pluginZip = createPluginUrl("fake", pluginDir);

        installPlugin(pluginZip, env.v1());
        assertPlugin("fake", pluginDir, env.v2());

        final Path fake = env.v2().pluginsFile().resolve("fake");
        final Path resources = fake.resolve("resources");
        final Path platform = fake.resolve("platform");
        final Path platformName = platform.resolve("linux-x86_64");
        final Path bin = platformName.resolve("bin");
        assert755(fake);
        assert644(fake.resolve("fake-" + Version.CURRENT + ".jar"));
        assert755(resources);
        assert644(resources.resolve("resource"));
        assert755(platform);
        assert755(platformName);
        assert755(bin.resolve("fake_executable"));
    }

    private void assert644(final Path path) throws IOException {
        final Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE));
        assertFalse(permissions.contains(PosixFilePermission.OWNER_EXECUTE));
        assertTrue(permissions.contains(PosixFilePermission.GROUP_READ));
        assertFalse(permissions.contains(PosixFilePermission.GROUP_WRITE));
        assertFalse(permissions.contains(PosixFilePermission.GROUP_EXECUTE));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_READ));
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_EXECUTE));
    }

    private void assert755(final Path path) throws IOException {
        final Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_EXECUTE));
        assertTrue(permissions.contains(PosixFilePermission.GROUP_READ));
        assertFalse(permissions.contains(PosixFilePermission.GROUP_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.GROUP_EXECUTE));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_READ));
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_EXECUTE));
    }

    public void testConfig() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path configDir = pluginDir.resolve("config");
        Files.createDirectory(configDir);
        Files.createFile(configDir.resolve("custom.yml"));
        String pluginZip = createPluginUrl("fake", pluginDir);
        installPlugin(pluginZip, env.v1());
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testExistingConfig() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path envConfigDir = env.v2().configFile().resolve("fake");
        Files.createDirectories(envConfigDir);
        Files.write(envConfigDir.resolve("custom.yml"), "existing config".getBytes(StandardCharsets.UTF_8));
        Path pluginDir = createPluginDir(temp);
        Path configDir = pluginDir.resolve("config");
        Files.createDirectory(configDir);
        Files.write(configDir.resolve("custom.yml"), "new config".getBytes(StandardCharsets.UTF_8));
        Files.createFile(configDir.resolve("other.yml"));
        String pluginZip = createPluginUrl("fake", pluginDir);
        installPlugin(pluginZip, env.v1());
        assertPlugin("fake", pluginDir, env.v2());
        List<String> configLines = Files.readAllLines(envConfigDir.resolve("custom.yml"), StandardCharsets.UTF_8);
        assertEquals(1, configLines.size());
        assertEquals("existing config", configLines.get(0));
        assertTrue(Files.exists(envConfigDir.resolve("other.yml")));
    }

    public void testConfigNotDir() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path configDir = pluginDir.resolve("config");
        Files.createFile(configDir);
        String pluginZip = createPluginUrl("fake", pluginDir);
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("not a directory"));
        assertInstallCleaned(env.v2());
    }

    public void testConfigContainsDir() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path dirInConfigDir = pluginDir.resolve("config").resolve("foo");
        Files.createDirectories(dirInConfigDir);
        Files.createFile(dirInConfigDir.resolve("myconfig.yml"));
        String pluginZip = createPluginUrl("fake", pluginDir);
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("Directories not allowed in config dir for plugin"));
        assertInstallCleaned(env.v2());
    }

    public void testConfigConflict() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path configDir = pluginDir.resolve("config");
        Files.createDirectory(configDir);
        Files.createFile(configDir.resolve("myconfig.yml"));
        String pluginZip = createPluginUrl("elasticsearch.yml", pluginDir);
        FileAlreadyExistsException e = expectThrows(FileAlreadyExistsException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains(env.v2().configFile().resolve("elasticsearch.yml").toString()));
        assertInstallCleaned(env.v2());
    }

    public void testMissingDescriptor() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Files.createFile(pluginDir.resolve("fake.yml"));
        String pluginZip = writeZip(pluginDir, "elasticsearch").toUri().toURL().toString();
        NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("plugin-descriptor.properties"));
        assertInstallCleaned(env.v2());
    }

    public void testMissingDirectory() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Files.createFile(pluginDir.resolve(PluginInfo.ES_PLUGIN_PROPERTIES));
        String pluginZip = writeZip(pluginDir, null).toUri().toURL().toString();
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip, env.v1()));
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
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip, env.v1()));
        assertTrue(e.getMessage(), e.getMessage().contains("resolving outside of plugin directory"));
    }

    public void testOfficialPluginsHelpSorted() throws Exception {
        MockTerminal terminal = new MockTerminal();
        new InstallPluginCommand() {
            @Override
            protected boolean addShutdownHook() {
                return false;
            }
        }.main(new String[] { "--help" }, terminal);
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
        new InstallPluginCommand() {
            @Override
            protected boolean addShutdownHook() {
                return false;
            }
        }.main(new String[] { "--help" }, terminal);
        assertTrue(terminal.getOutput(), terminal.getOutput().contains("x-pack"));
    }

    public void testInstallMisspelledOfficialPlugins() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        UserException e = expectThrows(UserException.class, () -> installPlugin("xpack", env.v1()));
        assertThat(e.getMessage(), containsString("Unknown plugin xpack, did you mean [x-pack]?"));

        e = expectThrows(UserException.class, () -> installPlugin("analysis-smartnc", env.v1()));
        assertThat(e.getMessage(), containsString("Unknown plugin analysis-smartnc, did you mean [analysis-smartcn]?"));

        e = expectThrows(UserException.class, () -> installPlugin("repository", env.v1()));
        assertThat(e.getMessage(), containsString("Unknown plugin repository, did you mean any of [repository-s3, repository-gcs]?"));

        e = expectThrows(UserException.class, () -> installPlugin("unknown_plugin", env.v1()));
        assertThat(e.getMessage(), containsString("Unknown plugin unknown_plugin"));
    }

    public void testBatchFlag() throws Exception {
        MockTerminal terminal = new MockTerminal();
        installPlugin(terminal, true);
        assertThat(terminal.getOutput(), containsString("WARNING: plugin requires additional permissions"));
    }

    public void testQuietFlagDisabled() throws Exception {
        MockTerminal terminal = new MockTerminal();
        terminal.setVerbosity(randomFrom(Terminal.Verbosity.NORMAL, Terminal.Verbosity.VERBOSE));
        installPlugin(terminal, false);
        assertThat(terminal.getOutput(), containsString("100%"));
    }

    public void testQuietFlagEnabled() throws Exception {
        MockTerminal terminal = new MockTerminal();
        terminal.setVerbosity(Terminal.Verbosity.SILENT);
        installPlugin(terminal, false);
        assertThat(terminal.getOutput(), not(containsString("100%")));
    }

    public void testPluginAlreadyInstalled() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPluginUrl("fake", pluginDir);
        installPlugin(pluginZip, env.v1());
        final UserException e = expectThrows(UserException.class,
            () -> installPlugin(pluginZip, env.v1(), randomFrom(skipJarHellCommand, defaultCommand)));
        assertThat(
            e.getMessage(),
            equalTo("plugin directory [" + env.v2().pluginsFile().resolve("fake") + "] already exists; " +
                "if you need to update the plugin, uninstall it first using command 'remove fake'"));
    }

    private void installPlugin(MockTerminal terminal, boolean isBatch) throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        // if batch is enabled, we also want to add a security policy
        String pluginZip = createPlugin("fake", pluginDir, isBatch).toUri().toURL().toString();
        skipJarHellCommand.execute(terminal, pluginZip, isBatch, env.v2());
    }

    public MockTerminal assertInstallPluginFromUrl(String pluginId, String name, String url, String stagingHash,
                                                   String shaExtension, Function<byte[], String> shaCalculator) throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        Path pluginZip = createPlugin(name, pluginDir, false);
        InstallPluginCommand command = new InstallPluginCommand() {
            @Override
            Path downloadZip(Terminal terminal, String urlString, Path tmpDir) throws IOException {
                assertEquals(url, urlString);
                Path downloadedPath = tmpDir.resolve("downloaded.zip");
                Files.copy(pluginZip, downloadedPath);
                return downloadedPath;
            }
            @Override
            URL openUrl(String urlString) throws Exception {
                String expectedUrl = url + shaExtension;
                if (expectedUrl.equals(urlString)) {
                    // calc sha an return file URL to it
                    Path shaFile = temp.apply("shas").resolve("downloaded.zip" + shaExtension);
                    byte[] zipbytes = Files.readAllBytes(pluginZip);
                    String checksum = shaCalculator.apply(zipbytes);
                    Files.write(shaFile, checksum.getBytes(StandardCharsets.UTF_8));
                    return shaFile.toUri().toURL();
                }
                return null;
            }
            @Override
            boolean urlExists(Terminal terminal, String urlString) throws IOException {
                return urlString.equals(url);
            }
            @Override
            String getStagingHash() {
                return stagingHash;
            }
            @Override
            void jarHellCheck(Path candidate, Path pluginsDir) throws Exception {
                // no jarhell check
            }
        };
        MockTerminal terminal = installPlugin(pluginId, env.v1(), command);
        assertPlugin(name, pluginDir, env.v2());
        return terminal;
    }

    public void assertInstallPluginFromUrl(String pluginId, String name, String url, String stagingHash) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-512");
        assertInstallPluginFromUrl(pluginId, name, url, stagingHash, ".sha512", checksumAndFilename(digest, url));
    }

    public void testOfficalPlugin() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-" + Version.CURRENT + ".zip";
        assertInstallPluginFromUrl("analysis-icu", "analysis-icu", url, null);
    }

    public void testOfficalPluginStaging() throws Exception {
        String url = "https://staging.elastic.co/" + Version.CURRENT + "-abc123/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-"
            + Version.CURRENT + ".zip";
        assertInstallPluginFromUrl("analysis-icu", "analysis-icu", url, "abc123");
    }

    public void testOfficalPlatformPlugin() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-" + Platforms.PLATFORM_NAME +
            "-" + Version.CURRENT + ".zip";
        assertInstallPluginFromUrl("analysis-icu", "analysis-icu", url, null);
    }

    public void testOfficalPlatformPluginStaging() throws Exception {
        String url = "https://staging.elastic.co/" + Version.CURRENT + "-abc123/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-"
            + Platforms.PLATFORM_NAME + "-"+ Version.CURRENT + ".zip";
        assertInstallPluginFromUrl("analysis-icu", "analysis-icu", url, "abc123");
    }

    public void testMavenPlugin() throws Exception {
        String url = "https://repo1.maven.org/maven2/mygroup/myplugin/1.0.0/myplugin-1.0.0.zip";
        assertInstallPluginFromUrl("mygroup:myplugin:1.0.0", "myplugin", url, null);
    }

    public void testMavenPlatformPlugin() throws Exception {
        String url = "https://repo1.maven.org/maven2/mygroup/myplugin/1.0.0/myplugin-" + Platforms.PLATFORM_NAME + "-1.0.0.zip";
        assertInstallPluginFromUrl("mygroup:myplugin:1.0.0", "myplugin", url, null);
    }

    public void testMavenSha1Backcompat() throws Exception {
        String url = "https://repo1.maven.org/maven2/mygroup/myplugin/1.0.0/myplugin-1.0.0.zip";
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        MockTerminal terminal = assertInstallPluginFromUrl("mygroup:myplugin:1.0.0", "myplugin", url, null, ".sha1", checksum(digest));
        assertTrue(terminal.getOutput(), terminal.getOutput().contains("sha512 not found, falling back to sha1"));
    }

    public void testOfficialShaMissing() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-" + Version.CURRENT + ".zip";
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        UserException e = expectThrows(UserException.class, () ->
            assertInstallPluginFromUrl("analysis-icu", "analysis-icu", url, null, ".sha1", checksum(digest)));
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertEquals("Plugin checksum missing: " + url + ".sha512", e.getMessage());
    }

    public void testMavenShaMissing() throws Exception {
        String url = "https://repo1.maven.org/maven2/mygroup/myplugin/1.0.0/myplugin-1.0.0.zip";
        UserException e = expectThrows(UserException.class, () ->
            assertInstallPluginFromUrl("mygroup:myplugin:1.0.0", "myplugin", url, null, ".dne", bytes -> null));
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertEquals("Plugin checksum missing: " + url + ".sha1", e.getMessage());
    }

    public void testInvalidShaFileMissingFilename() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-" + Version.CURRENT + ".zip";
        MessageDigest digest = MessageDigest.getInstance("SHA-512");
        UserException e = expectThrows(UserException.class, () ->
                assertInstallPluginFromUrl("analysis-icu", "analysis-icu", url, null, ".sha512", checksum(digest)));
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().startsWith("Invalid checksum file"));
    }

    public void testInvalidShaFileMismatchFilename() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-" + Version.CURRENT + ".zip";
        MessageDigest digest = MessageDigest.getInstance("SHA-512");
        UserException e = expectThrows(UserException.class, () ->
                assertInstallPluginFromUrl(
                        "analysis-icu",
                        "analysis-icu",
                        url,
                        null,
                        ".sha512",
                        checksumAndString(digest, "  repository-s3-" + Version.CURRENT + ".zip")));
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e, hasToString(matches("checksum file at \\[.*\\] is not for this plugin")));
    }

    public void testInvalidShaFileContainingExtraLine() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-" + Version.CURRENT + ".zip";
        MessageDigest digest = MessageDigest.getInstance("SHA-512");
        UserException e = expectThrows(UserException.class, () ->
            assertInstallPluginFromUrl(
                    "analysis-icu",
                    "analysis-icu",
                    url,
                    null,
                    ".sha512",
                    checksumAndString(digest, "  analysis-icu-" + Version.CURRENT + ".zip\nfoobar")));
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().startsWith("Invalid checksum file"));
    }

    public void testSha512Mismatch() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-" + Version.CURRENT + ".zip";
        UserException e = expectThrows(UserException.class, () ->
            assertInstallPluginFromUrl(
                    "analysis-icu",
                    "analysis-icu",
                    url,
                    null,
                    ".sha512",
                    bytes -> "foobar  analysis-icu-" + Version.CURRENT + ".zip"));
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("SHA-512 mismatch, expected foobar"));
    }

    public void testSha1Mismatch() throws Exception {
        String url = "https://repo1.maven.org/maven2/mygroup/myplugin/1.0.0/myplugin-1.0.0.zip";
        UserException e = expectThrows(UserException.class, () ->
            assertInstallPluginFromUrl("mygroup:myplugin:1.0.0", "myplugin", url, null, ".sha1", bytes -> "foobar"));
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertTrue(e.getMessage(), e.getMessage().contains("SHA-1 mismatch, expected foobar"));
    }

    public void testKeystoreNotRequired() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPluginUrl("fake", pluginDir, "requires.keystore", "false");
        installPlugin(pluginZip, env.v1());
        assertFalse(Files.exists(KeyStoreWrapper.keystorePath(env.v2().configFile())));
    }

    public void testKeystoreRequiredAlreadyExists() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        KeyStoreWrapper keystore = KeyStoreWrapper.create(new char[0]);
        keystore.save(env.v2().configFile());
        byte[] expectedBytes = Files.readAllBytes(KeyStoreWrapper.keystorePath(env.v2().configFile()));
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPluginUrl("fake", pluginDir, "requires.keystore", "true");
        installPlugin(pluginZip, env.v1());
        byte[] gotBytes = Files.readAllBytes(KeyStoreWrapper.keystorePath(env.v2().configFile()));
        assertArrayEquals("Keystore was modified", expectedBytes, gotBytes);
    }

    public void testKeystoreRequiredCreated() throws Exception {
        Tuple<Path, Environment> env = createEnv(fs, temp);
        Path pluginDir = createPluginDir(temp);
        String pluginZip = createPluginUrl("fake", pluginDir, "requires.keystore", "true");
        MockTerminal terminal = installPlugin(pluginZip, env.v1());
        assertTrue(Files.exists(KeyStoreWrapper.keystorePath(env.v2().configFile())));
    }

    private Function<byte[], String> checksum(final MessageDigest digest) {
        return checksumAndString(digest, "");
    }

    private Function<byte[], String> checksumAndFilename(final MessageDigest digest, final String url) throws MalformedURLException {
        final String[] segments = URI.create(url).getPath().split("/");
        return checksumAndString(digest, "  " + segments[segments.length - 1]);
    }

    private Function<byte[], String> checksumAndString(final MessageDigest digest, final String s) {
        return bytes -> MessageDigests.toHexString(digest.digest(bytes)) + s;
    }

}
