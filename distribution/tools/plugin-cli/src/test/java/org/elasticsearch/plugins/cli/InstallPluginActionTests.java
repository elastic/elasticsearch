/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.apache.lucene.util.LuceneTestCase;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.bcpg.BCPGOutputStream;
import org.bouncycastle.bcpg.HashAlgorithmTags;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPKeyPair;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureGenerator;
import org.bouncycastle.openpgp.operator.PGPDigestCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentSignerBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPDigestCalculatorProviderBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPKeyPair;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyEncryptorBuilder;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginTestUtil;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PosixPermissionsResetter;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
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
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.forEachFileRecursively;
import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems("*")
public class InstallPluginActionTests extends ESTestCase {

    private InstallPluginAction skipJarHellAction;
    private InstallPluginAction defaultAction;

    private final Function<String, Path> temp;
    private MockTerminal terminal;
    private Tuple<Path, Environment> env;
    private Path pluginDir;

    private final boolean isPosix;
    private final boolean isReal;
    private final String javaIoTmpdir;

    @SuppressForbidden(reason = "sets java.io.tmpdir")
    public InstallPluginActionTests(FileSystem fs, Function<String, Path> temp) {
        this.temp = temp;
        this.isPosix = fs.supportedFileAttributeViews().contains("posix");
        this.isReal = fs == PathUtils.getDefaultFileSystem();
        PathUtilsForTesting.installMock(fs);
        javaIoTmpdir = System.getProperty("java.io.tmpdir");
        System.setProperty("java.io.tmpdir", temp.apply("tmpdir").toString());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        pluginDir = createPluginDir(temp);
        terminal = new MockTerminal();
        env = createEnv(temp);
        skipJarHellAction = new InstallPluginAction(terminal, null, false) {
            @Override
            void jarHellCheck(PluginDescriptor candidateInfo, Path candidate, Path pluginsDir, Path modulesDir) {
                // no jarhell check
            }
        };
        defaultAction = new InstallPluginAction(terminal, env.v2(), false);
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
            forEachFileRecursively(structure, (file, attrs) -> {
                String target = (prefix == null ? "" : prefix + "/") + structure.relativize(file);
                stream.putNextEntry(new ZipEntry(target));
                Files.copy(file, stream);
            });
        }
        return zip;
    }

    /** creates a plugin .zip and returns the url for testing */
    static InstallablePlugin createPluginZip(String name, Path structure, String... additionalProps) throws IOException {
        return createPlugin(name, structure, additionalProps);
    }

    static void writePlugin(String name, Path structure, String... additionalProps) throws IOException {
        String[] properties = Stream.concat(
            Stream.of(
                "description",
                "fake desc",
                "name",
                name,
                "version",
                "1.0",
                "elasticsearch.version",
                Version.CURRENT.toString(),
                "java.version",
                System.getProperty("java.specification.version"),
                "classname",
                "FakePlugin"
            ),
            Arrays.stream(additionalProps)
        ).toArray(String[]::new);
        PluginTestUtil.writePluginProperties(structure, properties);
        String className = name.substring(0, 1).toUpperCase(Locale.ENGLISH) + name.substring(1) + "Plugin";
        writeJar(structure.resolve("plugin.jar"), className);
    }

    static void writePluginSecurityPolicy(Path pluginDir, String... permissions) throws IOException {
        StringBuilder securityPolicyContent = new StringBuilder("grant {\n  ");
        for (String permission : permissions) {
            securityPolicyContent.append("permission java.lang.RuntimePermission \"");
            securityPolicyContent.append(permission);
            securityPolicyContent.append("\";");
        }
        securityPolicyContent.append("\n};\n");
        Files.write(pluginDir.resolve("plugin-security.policy"), securityPolicyContent.toString().getBytes(StandardCharsets.UTF_8));
    }

    static InstallablePlugin createPlugin(String name, Path structure, String... additionalProps) throws IOException {
        writePlugin(name, structure, additionalProps);
        return new InstallablePlugin(name, writeZip(structure, null).toUri().toURL().toString());
    }

    void installPlugin(String id) throws Exception {
        InstallablePlugin plugin = id == null ? null : new InstallablePlugin(id, id);
        installPlugin(plugin, env.v1(), skipJarHellAction);
    }

    void installPlugin(InstallablePlugin plugin) throws Exception {
        installPlugin(plugin, env.v1(), skipJarHellAction);
    }

    void installPlugins(final List<InstallablePlugin> plugins, final Path home) throws Exception {
        installPlugins(plugins, home, skipJarHellAction);
    }

    void installPlugin(InstallablePlugin plugin, Path home, InstallPluginAction action) throws Exception {
        installPlugins(plugin == null ? Collections.emptyList() : Collections.singletonList(plugin), home, action);
    }

    void installPlugins(final List<InstallablePlugin> plugins, final Path home, final InstallPluginAction action) throws Exception {
        final Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", home).build());
        action.setEnvironment(environment);
        action.execute(plugins);
    }

    void assertPlugin(String name, Path original, Environment environment) throws IOException {
        assertPluginInternal(name, environment.pluginsFile(), original);
        assertConfigAndBin(name, original, environment);
        assertInstallCleaned(environment);
    }

    void assertPluginInternal(String name, Path pluginsFile, Path originalPlugin) throws IOException {
        Path got = pluginsFile.resolve(name);
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
                    PosixFilePermission.OTHERS_EXECUTE
                )
            );
        }
        try (Stream<Path> files = Files.list(originalPlugin).filter(p -> p.getFileName().toString().endsWith(".jar"))) {
            files.forEach(file -> {
                Path expectedJar = got.resolve(originalPlugin.relativize(file).toString());
                assertTrue("jar [" + file.getFileName() + "] was copied", Files.exists(expectedJar));
            });
        }
        assertFalse("bin was not copied", Files.exists(got.resolve("bin")));
        assertFalse("config was not copied", Files.exists(got.resolve("config")));
    }

    void assertConfigAndBin(String name, Path original, Environment environment) throws IOException {
        if (Files.exists(original.resolve("bin"))) {
            Path binDir = environment.binFile().resolve(name);
            assertTrue("bin dir exists", Files.exists(binDir));
            assertTrue("bin is a dir", Files.isDirectory(binDir));
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(binDir)) {
                for (Path file : stream) {
                    assertFalse("not a dir", Files.isDirectory(file));
                    if (isPosix) {
                        PosixFileAttributes attributes = Files.readAttributes(file, PosixFileAttributes.class);
                        assertEquals(InstallPluginAction.BIN_FILES_PERMS, attributes.permissions());
                    }
                }
            }
        }
        if (Files.exists(original.resolve("config"))) {
            Path configDir = environment.configFile().resolve(name);
            assertTrue("config dir exists", Files.exists(configDir));
            assertTrue("config is a dir", Files.isDirectory(configDir));

            UserPrincipal user = null;
            GroupPrincipal group = null;

            if (isPosix) {
                PosixFileAttributes configAttributes = Files.getFileAttributeView(environment.configFile(), PosixFileAttributeView.class)
                    .readAttributes();
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
    }

    void assertInstallCleaned(Environment environment) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(environment.pluginsFile())) {
            for (Path file : stream) {
                if (file.getFileName().toString().startsWith(".installing")) {
                    fail("Installation dir still exists, " + file);
                }
            }
        }
    }

    public void testMissingPluginId() {
        final UserException e = expectThrows(UserException.class, () -> installPlugin((String) null));
        assertTrue(e.getMessage(), e.getMessage().contains("at least one plugin id is required"));
    }

    public void testSomethingWorks() throws Exception {
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        installPlugin(pluginZip);
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testMultipleWorks() throws Exception {
        InstallablePlugin fake1PluginZip = createPluginZip("fake1", pluginDir);
        InstallablePlugin fake2PluginZip = createPluginZip("fake2", pluginDir);
        installPlugins(Arrays.asList(fake1PluginZip, fake2PluginZip), env.v1());
        assertPlugin("fake1", pluginDir, env.v2());
        assertPlugin("fake2", pluginDir, env.v2());
    }

    public void testDuplicateInstall() throws Exception {
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        final UserException e = expectThrows(UserException.class, () -> installPlugins(Arrays.asList(pluginZip, pluginZip), env.v1()));
        assertThat(e.getMessage(), equalTo("duplicate plugin id [" + pluginZip.getId() + "]"));
    }

    public void testTransaction() throws Exception {
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        InstallablePlugin nonexistentPluginZip = new InstallablePlugin(
            pluginZip.getId() + "-does-not-exist",
            pluginZip.getLocation() + "-does-not-exist"
        );
        final FileNotFoundException e = expectThrows(
            FileNotFoundException.class,
            () -> installPlugins(Arrays.asList(pluginZip, nonexistentPluginZip), env.v1())
        );
        assertThat(e.getMessage(), containsString("does-not-exist"));
        final Path fakeInstallPath = env.v2().pluginsFile().resolve("fake");
        // fake should have been removed when the file not found exception occurred
        assertFalse(Files.exists(fakeInstallPath));
        assertInstallCleaned(env.v2());
    }

    public void testInstallFailsIfPreviouslyRemovedPluginFailed() throws Exception {
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        final Path removing = env.v2().pluginsFile().resolve(".removing-failed");
        Files.createDirectory(removing);
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> installPlugin(pluginZip));
        final String expected = String.format(
            Locale.ROOT,
            "found file [%s] from a failed attempt to remove the plugin [failed]; execute [elasticsearch-plugin remove failed]",
            removing
        );
        assertThat(e.getMessage(), containsString(expected));
    }

    public void testSpaceInUrl() throws Exception {
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        Path pluginZipWithSpaces = createTempFile("foo bar", ".zip");
        try (InputStream in = FileSystemUtils.openFileURLStream(new URL(pluginZip.getLocation()))) {
            Files.copy(in, pluginZipWithSpaces, StandardCopyOption.REPLACE_EXISTING);
        }
        InstallablePlugin modifiedPlugin = new InstallablePlugin("fake", pluginZipWithSpaces.toUri().toURL().toString());
        installPlugin(modifiedPlugin);
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testMalformedUrlNotMaven() {
        // has two colons, so it appears similar to maven coordinates
        InstallablePlugin plugin = new InstallablePlugin("fake", "://host:1234");
        MalformedURLException e = expectThrows(MalformedURLException.class, () -> installPlugin(plugin));
        assertThat(e.getMessage(), containsString("no protocol"));
    }

    public void testFileNotMaven() {
        String dir = randomAlphaOfLength(10) + ":" + randomAlphaOfLength(5) + "\\" + randomAlphaOfLength(5);
        Exception e = expectThrows(
            Exception.class,
            // has two colons, so it appears similar to maven coordinates
            () -> installPlugin("file:" + dir)
        );
        assertThat(e.getMessage(), not(containsString("maven.org")));
        assertThat(e.getMessage(), containsString(dir));
    }

    public void testUnknownPlugin() {
        UserException e = expectThrows(UserException.class, () -> installPlugin("foo"));
        assertThat(e.getMessage(), containsString("Unknown plugin foo"));
    }

    public void testPluginsDirReadOnly() throws Exception {
        assumeTrue("posix and filesystem", isPosix && isReal);
        try (PosixPermissionsResetter pluginsAttrs = new PosixPermissionsResetter(env.v2().pluginsFile())) {
            pluginsAttrs.setPermissions(new HashSet<>());
            InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
            IOException e = expectThrows(IOException.class, () -> installPlugin(pluginZip));
            assertThat(e.getMessage(), containsString(env.v2().pluginsFile().toString()));
        }
        assertInstallCleaned(env.v2());
    }

    public void testBuiltinModule() throws Exception {
        InstallablePlugin pluginZip = createPluginZip("lang-painless", pluginDir);
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip));
        assertThat(e.getMessage(), containsString("is a system module"));
        assertInstallCleaned(env.v2());
    }

    public void testBuiltinXpackModule() throws Exception {
        InstallablePlugin pluginZip = createPluginZip("x-pack", pluginDir);
        // There is separate handling for installing "x-pack", versus installing a plugin
        // whose descriptor contains the name "x-pack".
        pluginZip.setId("not-x-pack");
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip));
        assertThat(e.getMessage(), containsString("is a system module"));
        assertInstallCleaned(env.v2());
    }

    public void testJarHell() throws Exception {
        // jar hell test needs a real filesystem
        assumeTrue("real filesystem", isReal);
        Path pluginDirectory = createPluginDir(temp);
        writeJar(pluginDirectory.resolve("other.jar"), "FakePlugin");
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDirectory); // adds plugin.jar with FakePlugin
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> installPlugin(pluginZip, env.v1(), defaultAction));
        assertThat(e.getMessage(), containsString("jar hell"));
        assertInstallCleaned(env.v2());
    }

    public void testIsolatedPlugins() throws Exception {
        // these both share the same FakePlugin class
        Path pluginDir1 = createPluginDir(temp);
        InstallablePlugin pluginZip1 = createPluginZip("fake1", pluginDir1);
        installPlugin(pluginZip1);
        Path pluginDir2 = createPluginDir(temp);
        InstallablePlugin pluginZip2 = createPluginZip("fake2", pluginDir2);
        installPlugin(pluginZip2);
        assertPlugin("fake1", pluginDir1, env.v2());
        assertPlugin("fake2", pluginDir2, env.v2());
    }

    public void testExistingPlugin() throws Exception {
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        installPlugin(pluginZip);
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip));
        assertThat(e.getMessage(), containsString("already exists"));
        assertInstallCleaned(env.v2());
    }

    public void testBin() throws Exception {
        Path binDir = pluginDir.resolve("bin");
        Files.createDirectory(binDir);
        Files.createFile(binDir.resolve("somescript"));
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        installPlugin(pluginZip);
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testBinNotDir() throws Exception {
        Path binDir = pluginDir.resolve("bin");
        Files.createFile(binDir);
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip));
        assertThat(e.getMessage(), containsString("not a directory"));
        assertInstallCleaned(env.v2());
    }

    public void testBinContainsDir() throws Exception {
        Path dirInBinDir = pluginDir.resolve("bin").resolve("foo");
        Files.createDirectories(dirInBinDir);
        Files.createFile(dirInBinDir.resolve("somescript"));
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip));
        assertThat(e.getMessage(), containsString("Directories not allowed in bin dir for plugin"));
        assertInstallCleaned(env.v2());
    }

    public void testBinConflict() throws Exception {
        Path binDir = pluginDir.resolve("bin");
        Files.createDirectory(binDir);
        Files.createFile(binDir.resolve("somescript"));
        InstallablePlugin pluginZip = createPluginZip("elasticsearch", pluginDir);
        FileAlreadyExistsException e = expectThrows(FileAlreadyExistsException.class, () -> installPlugin(pluginZip));
        assertThat(e.getMessage(), containsString(env.v2().binFile().resolve("elasticsearch").toString()));
        assertInstallCleaned(env.v2());
    }

    public void testBinPermissions() throws Exception {
        assumeTrue("posix filesystem", isPosix);
        Path binDir = pluginDir.resolve("bin");
        Files.createDirectory(binDir);
        Files.createFile(binDir.resolve("somescript"));
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        try (PosixPermissionsResetter binAttrs = new PosixPermissionsResetter(env.v2().binFile())) {
            Set<PosixFilePermission> perms = binAttrs.getCopyPermissions();
            // make sure at least one execute perm is missing, so we know we forced it during installation
            perms.remove(PosixFilePermission.GROUP_EXECUTE);
            binAttrs.setPermissions(perms);
            installPlugin(pluginZip);
            assertPlugin("fake", pluginDir, env.v2());
        }
    }

    public void testPluginPermissions() throws Exception {
        assumeTrue("posix filesystem", isPosix);

        final Path tempPluginDir = createPluginDir(temp);
        final Path resourcesDir = tempPluginDir.resolve("resources");
        final Path platformDir = tempPluginDir.resolve("platform");
        final Path platformNameDir = platformDir.resolve("linux-x86_64");
        final Path platformBinDir = platformNameDir.resolve("bin");
        Files.createDirectories(platformBinDir);

        Files.createFile(tempPluginDir.resolve("fake-" + Version.CURRENT.toString() + ".jar"));
        Files.createFile(platformBinDir.resolve("fake_executable"));
        Files.createDirectory(resourcesDir);
        Files.createFile(resourcesDir.resolve("resource"));

        final InstallablePlugin pluginZip = createPluginZip("fake", tempPluginDir);

        installPlugin(pluginZip);
        assertPlugin("fake", tempPluginDir, env.v2());

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
        Path configDir = pluginDir.resolve("config");
        Files.createDirectory(configDir);
        Files.createFile(configDir.resolve("custom.yml"));
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        installPlugin(pluginZip);
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testExistingConfig() throws Exception {
        Path envConfigDir = env.v2().configFile().resolve("fake");
        Files.createDirectories(envConfigDir);
        Files.write(envConfigDir.resolve("custom.yml"), "existing config".getBytes(StandardCharsets.UTF_8));
        Path configDir = pluginDir.resolve("config");
        Files.createDirectory(configDir);
        Files.write(configDir.resolve("custom.yml"), "new config".getBytes(StandardCharsets.UTF_8));
        Files.createFile(configDir.resolve("other.yml"));
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        installPlugin(pluginZip);
        assertPlugin("fake", pluginDir, env.v2());
        List<String> configLines = Files.readAllLines(envConfigDir.resolve("custom.yml"), StandardCharsets.UTF_8);
        assertEquals(1, configLines.size());
        assertEquals("existing config", configLines.get(0));
        assertTrue(Files.exists(envConfigDir.resolve("other.yml")));
    }

    public void testConfigNotDir() throws Exception {
        Files.createDirectories(pluginDir);
        Path configDir = pluginDir.resolve("config");
        Files.createFile(configDir);
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip));
        assertThat(e.getMessage(), containsString("not a directory"));
        assertInstallCleaned(env.v2());
    }

    public void testConfigContainsDir() throws Exception {
        Path dirInConfigDir = pluginDir.resolve("config").resolve("foo");
        Files.createDirectories(dirInConfigDir);
        Files.createFile(dirInConfigDir.resolve("myconfig.yml"));
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip));
        assertThat(e.getMessage(), containsString("Directories not allowed in config dir for plugin"));
        assertInstallCleaned(env.v2());
    }

    public void testMissingDescriptor() throws Exception {
        Files.createFile(pluginDir.resolve("fake.yml"));
        String pluginZip = writeZip(pluginDir, null).toUri().toURL().toString();
        NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> installPlugin(pluginZip));
        assertThat(e.getMessage(), containsString("plugin-descriptor.properties"));
        assertInstallCleaned(env.v2());
    }

    public void testContainsIntermediateDirectory() throws Exception {
        Files.createFile(pluginDir.resolve(PluginDescriptor.ES_PLUGIN_PROPERTIES));
        String pluginZip = writeZip(pluginDir, "elasticsearch").toUri().toURL().toString();
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip));
        assertThat(e.getMessage(), containsString("This plugin was built with an older plugin structure"));
        assertInstallCleaned(env.v2());
    }

    public void testZipRelativeOutsideEntryName() throws Exception {
        Path zip = createTempDir().resolve("broken.zip");
        try (ZipOutputStream stream = new ZipOutputStream(Files.newOutputStream(zip))) {
            stream.putNextEntry(new ZipEntry("../blah"));
        }
        String pluginZip = zip.toUri().toURL().toString();
        UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip));
        assertThat(e.getMessage(), containsString("resolving outside of plugin directory"));
        assertInstallCleaned(env.v2());
    }

    public void testOfficialPluginsHelpSortedAndMissingObviouslyWrongPlugins() throws Exception {
        MockTerminal mockTerminal = new MockTerminal();
        new MockInstallPluginCommand().main(new String[] { "--help" }, mockTerminal);
        try (BufferedReader reader = new BufferedReader(new StringReader(mockTerminal.getOutput()))) {
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
                // qa is not really a plugin and it shouldn't sneak in
                assertThat(line, not(endsWith("qa")));
                assertThat(line, not(endsWith("example")));
            }
        }
    }

    public void testInstallXPack() throws IOException {
        runInstallXPackTest(Build.Flavor.DEFAULT, UserException.class, "this distribution of Elasticsearch contains X-Pack by default");
        runInstallXPackTest(
            Build.Flavor.OSS,
            UserException.class,
            "X-Pack is not available with the oss distribution; to use X-Pack features use the default distribution"
        );
        runInstallXPackTest(Build.Flavor.UNKNOWN, IllegalStateException.class, "your distribution is broken");
    }

    private <T extends Exception> void runInstallXPackTest(final Build.Flavor flavor, final Class<T> clazz, final String expectedMessage)
        throws IOException {

        final Environment environment = createEnv(temp).v2();
        final InstallPluginAction flavorAction = new InstallPluginAction(terminal, environment, false) {
            @Override
            Build.Flavor buildFlavor() {
                return flavor;
            }
        };
        final T exception = expectThrows(clazz, () -> flavorAction.execute(Collections.singletonList(new InstallablePlugin("x-pack"))));
        assertThat(exception.getMessage(), containsString(expectedMessage));
    }

    public void testInstallMisspelledOfficialPlugins() {
        UserException e = expectThrows(UserException.class, () -> installPlugin("analysis-smartnc"));
        assertThat(e.getMessage(), containsString("Unknown plugin analysis-smartnc, did you mean [analysis-smartcn]?"));

        e = expectThrows(UserException.class, () -> installPlugin("repository"));
        assertThat(e.getMessage(), containsString("Unknown plugin repository, did you mean any of [repository-s3, repository-gcs]?"));

        e = expectThrows(UserException.class, () -> installPlugin("unknown_plugin"));
        assertThat(e.getMessage(), containsString("Unknown plugin unknown_plugin"));
    }

    public void testBatchFlag() throws Exception {
        installPlugin(true);
        assertThat(terminal.getErrorOutput(), containsString("WARNING: plugin requires additional permissions"));
        assertThat(terminal.getOutput(), containsString("-> Downloading"));
        // No progress bar in batch mode
        assertThat(terminal.getOutput(), not(containsString("100%")));
    }

    public void testQuietFlagDisabled() throws Exception {
        terminal.setVerbosity(randomFrom(Terminal.Verbosity.NORMAL, Terminal.Verbosity.VERBOSE));
        installPlugin(false);
        assertThat(terminal.getOutput(), containsString("100%"));
    }

    public void testQuietFlagEnabled() throws Exception {
        terminal.setVerbosity(Terminal.Verbosity.SILENT);
        installPlugin(false);
        assertThat(terminal.getOutput(), not(containsString("100%")));
    }

    public void testPluginAlreadyInstalled() throws Exception {
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);
        installPlugin(pluginZip);
        final UserException e = expectThrows(
            UserException.class,
            () -> installPlugin(pluginZip, env.v1(), randomFrom(skipJarHellAction, defaultAction))
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "plugin directory ["
                    + env.v2().pluginsFile().resolve("fake")
                    + "] already exists; "
                    + "if you need to update the plugin, uninstall it first using command 'remove fake'"
            )
        );
    }

    /**
     * Check that if the installer action finds a mismatch between what it expects a plugin's ID to be and what
     * the ID actually is from the plugin's properties, then the installation fails.
     */
    public void testPluginHasDifferentNameThatDescriptor() throws Exception {
        InstallablePlugin descriptor = createPluginZip("fake", pluginDir);
        InstallablePlugin modifiedDescriptor = new InstallablePlugin("other-fake", descriptor.getLocation());

        final UserException e = expectThrows(UserException.class, () -> installPlugin(modifiedDescriptor));
        assertThat(e.getMessage(), equalTo("Expected downloaded plugin to have ID [other-fake] but found [fake]"));
    }

    private void installPlugin(boolean isBatch, String... additionalProperties) throws Exception {
        // if batch is enabled, we also want to add a security policy
        if (isBatch) {
            writePluginSecurityPolicy(pluginDir, "setFactory");
        }
        InstallablePlugin pluginZip = createPlugin("fake", pluginDir, additionalProperties);
        skipJarHellAction.setEnvironment(env.v2());
        skipJarHellAction.setBatch(isBatch);
        skipJarHellAction.execute(Collections.singletonList(pluginZip));
    }

    private void assertInstallPluginFromUrl(final String pluginId, final String url, final String stagingHash, boolean isSnapshot)
        throws Exception {
        assertInstallPluginFromUrl(pluginId, null, url, stagingHash, isSnapshot);
    }

    private void assertInstallPluginFromUrl(
        final String pluginId,
        final String pluginUrl,
        final String url,
        final String stagingHash,
        boolean isSnapshot
    ) throws Exception {
        final MessageDigest digest = MessageDigest.getInstance("SHA-512");
        assertInstallPluginFromUrl(
            pluginId,
            pluginUrl,
            url,
            stagingHash,
            isSnapshot,
            ".sha512",
            checksumAndFilename(digest, url),
            newSecretKey(),
            this::signature
        );
    }

    @SuppressForbidden(reason = "Paths.get() is OK in this context")
    void assertInstallPluginFromUrl(
        final String pluginId,
        final String pluginUrl,
        final String url,
        final String stagingHash,
        final boolean isSnapshot,
        final String shaExtension,
        final Function<byte[], String> shaCalculator,
        final PGPSecretKey secretKey,
        final BiFunction<byte[], PGPSecretKey, String> signature
    ) throws Exception {
        InstallablePlugin pluginZip = createPlugin(pluginId, pluginDir);
        Path pluginZipPath = Paths.get(URI.create(pluginZip.getLocation()));
        InstallPluginAction action = new InstallPluginAction(terminal, env.v2(), false) {
            @Override
            Path downloadZip(String urlString, Path tmpDir) throws IOException {
                assertEquals(url, urlString);
                Path downloadedPath = tmpDir.resolve("downloaded.zip");
                Files.copy(pluginZipPath, downloadedPath);
                return downloadedPath;
            }

            @Override
            URL openUrl(String urlString) throws IOException {
                if ((url + shaExtension).equals(urlString)) {
                    // calc sha and return file URL to it
                    Path shaFile = temp.apply("shas").resolve("downloaded.zip" + shaExtension);
                    byte[] zipbytes = Files.readAllBytes(pluginZipPath);
                    String checksum = shaCalculator.apply(zipbytes);
                    Files.write(shaFile, checksum.getBytes(StandardCharsets.UTF_8));
                    return shaFile.toUri().toURL();
                } else if ((url + ".asc").equals(urlString)) {
                    final Path ascFile = temp.apply("asc").resolve("downloaded.zip" + ".asc");
                    final byte[] zipBytes = Files.readAllBytes(pluginZipPath);
                    final String asc = signature.apply(zipBytes, secretKey);
                    Files.write(ascFile, asc.getBytes(StandardCharsets.UTF_8));
                    return ascFile.toUri().toURL();
                }
                return null;
            }

            @Override
            void verifySignature(Path zip, String urlString) throws IOException, PGPException {
                if (InstallPluginAction.OFFICIAL_PLUGINS.contains(pluginId)) {
                    super.verifySignature(zip, urlString);
                } else {
                    throw new UnsupportedOperationException("verify signature should not be called for unofficial plugins");
                }
            }

            @Override
            InputStream pluginZipInputStream(Path zip) throws IOException {
                return new ByteArrayInputStream(Files.readAllBytes(zip));
            }

            @Override
            String getPublicKeyId() {
                return Long.toHexString(secretKey.getKeyID()).toUpperCase(Locale.ROOT);
            }

            @Override
            InputStream getPublicKey() {
                try {
                    final ByteArrayOutputStream output = new ByteArrayOutputStream();
                    final ArmoredOutputStream armored = new ArmoredOutputStream(output);
                    secretKey.getPublicKey().encode(armored);
                    armored.close();
                    return new ByteArrayInputStream(output.toByteArray());
                } catch (final IOException e) {
                    throw new AssertionError(e);
                }
            }

            @Override
            boolean urlExists(String urlString) {
                return urlString.equals(url);
            }

            @Override
            String getStagingHash() {
                return stagingHash;
            }

            @Override
            boolean isSnapshot() {
                return isSnapshot;
            }

            @Override
            void jarHellCheck(PluginDescriptor candidateInfo, Path candidate, Path pluginsDir, Path modulesDir) {
                // no jarhell check
            }
        };
        installPlugin(new InstallablePlugin(pluginId, pluginUrl), env.v1(), action);
        assertPlugin(pluginId, pluginDir, env.v2());
    }

    public void testOfficialPlugin() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-"
            + Build.CURRENT.getQualifiedVersion()
            + ".zip";
        assertInstallPluginFromUrl("analysis-icu", url, null, false);
    }

    public void testOfficialPluginSnapshot() throws Exception {
        String url = String.format(
            Locale.ROOT,
            "https://snapshots.elastic.co/%s-abc123/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-%s.zip",
            Version.CURRENT,
            Build.CURRENT.getQualifiedVersion()
        );
        assertInstallPluginFromUrl("analysis-icu", url, "abc123", true);
    }

    public void testInstallReleaseBuildOfPluginOnSnapshotBuild() {
        String url = String.format(
            Locale.ROOT,
            "https://snapshots.elastic.co/%s-abc123/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-%s.zip",
            Version.CURRENT,
            Build.CURRENT.getQualifiedVersion()
        );
        // attempting to install a release build of a plugin (no staging ID) on a snapshot build should throw a user exception
        final UserException e = expectThrows(
            UserException.class,
            () -> assertInstallPluginFromUrl("analysis-icu", "analysis-icu", url, null, true)
        );
        assertThat(e.exitCode, equalTo(ExitCodes.CONFIG));
        assertThat(
            e.getMessage(),
            containsString("attempted to install release build of official plugin on snapshot build of Elasticsearch")
        );
    }

    public void testOfficialPluginStaging() throws Exception {
        String url = "https://staging.elastic.co/"
            + Version.CURRENT
            + "-abc123/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-"
            + Build.CURRENT.getQualifiedVersion()
            + ".zip";
        assertInstallPluginFromUrl("analysis-icu", url, "abc123", false);
    }

    public void testOfficialPlatformPlugin() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-"
            + Platforms.PLATFORM_NAME
            + "-"
            + Build.CURRENT.getQualifiedVersion()
            + ".zip";
        assertInstallPluginFromUrl("analysis-icu", url, null, false);
    }

    public void testOfficialPlatformPluginSnapshot() throws Exception {
        String url = String.format(
            Locale.ROOT,
            "https://snapshots.elastic.co/%s-abc123/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-%s-%s.zip",
            Version.CURRENT,
            Platforms.PLATFORM_NAME,
            Build.CURRENT.getQualifiedVersion()
        );
        assertInstallPluginFromUrl("analysis-icu", url, "abc123", true);
    }

    public void testOfficialPlatformPluginStaging() throws Exception {
        String url = "https://staging.elastic.co/"
            + Version.CURRENT
            + "-abc123/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-"
            + Platforms.PLATFORM_NAME
            + "-"
            + Build.CURRENT.getQualifiedVersion()
            + ".zip";
        assertInstallPluginFromUrl("analysis-icu", url, "abc123", false);
    }

    public void testMavenPlugin() throws Exception {
        String url = "https://repo1.maven.org/maven2/mygroup/myplugin/1.0.0/myplugin-1.0.0.zip";
        assertInstallPluginFromUrl("myplugin", "mygroup:myplugin:1.0.0", url, null, false);
    }

    public void testMavenPlatformPlugin() throws Exception {
        String url = "https://repo1.maven.org/maven2/mygroup/myplugin/1.0.0/myplugin-" + Platforms.PLATFORM_NAME + "-1.0.0.zip";
        assertInstallPluginFromUrl("myplugin", "mygroup:myplugin:1.0.0", url, null, false);
    }

    public void testMavenSha1Backcompat() throws Exception {
        String url = "https://repo1.maven.org/maven2/mygroup/myplugin/1.0.0/myplugin-1.0.0.zip";
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        assertInstallPluginFromUrl("myplugin", "mygroup:myplugin:1.0.0", url, null, false, ".sha1", checksum(digest), null, (b, p) -> null);
        assertTrue(terminal.getOutput(), terminal.getOutput().contains("sha512 not found, falling back to sha1"));
    }

    public void testMavenChecksumWithoutFilename() throws Exception {
        String url = "https://repo1.maven.org/maven2/mygroup/myplugin/1.0.0/myplugin-1.0.0.zip";
        MessageDigest digest = MessageDigest.getInstance("SHA-512");
        assertInstallPluginFromUrl(
            "myplugin",
            "mygroup:myplugin:1.0.0",
            url,
            null,
            false,
            ".sha512",
            checksum(digest),
            null,
            (b, p) -> null
        );
    }

    public void testOfficialChecksumWithoutFilename() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-"
            + Build.CURRENT.getQualifiedVersion()
            + ".zip";
        MessageDigest digest = MessageDigest.getInstance("SHA-512");
        UserException e = expectThrows(
            UserException.class,
            () -> assertInstallPluginFromUrl("analysis-icu", null, url, null, false, ".sha512", checksum(digest), null, (b, p) -> null)
        );
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e.getMessage(), startsWith("Invalid checksum file"));
    }

    public void testOfficialShaMissing() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-"
            + Build.CURRENT.getQualifiedVersion()
            + ".zip";
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        UserException e = expectThrows(
            UserException.class,
            () -> assertInstallPluginFromUrl("analysis-icu", null, url, null, false, ".sha1", checksum(digest), null, (b, p) -> null)
        );
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e.getMessage(), equalTo("Plugin checksum missing: " + url + ".sha512"));
    }

    public void testMavenShaMissing() {
        String url = "https://repo1.maven.org/maven2/mygroup/myplugin/1.0.0/myplugin-1.0.0.zip";
        UserException e = expectThrows(
            UserException.class,
            () -> assertInstallPluginFromUrl(
                "myplugin",
                "mygroup:myplugin:1.0.0",
                url,
                null,
                false,
                ".dne",
                bytes -> null,
                null,
                (b, p) -> null
            )
        );
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e.getMessage(), equalTo("Plugin checksum missing: " + url + ".sha1"));
    }

    public void testInvalidShaFileMissingFilename() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-"
            + Build.CURRENT.getQualifiedVersion()
            + ".zip";
        MessageDigest digest = MessageDigest.getInstance("SHA-512");
        UserException e = expectThrows(
            UserException.class,
            () -> assertInstallPluginFromUrl("analysis-icu", null, url, null, false, ".sha512", checksum(digest), null, (b, p) -> null)
        );
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("Invalid checksum file"));
    }

    public void testInvalidShaFileMismatchFilename() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-"
            + Build.CURRENT.getQualifiedVersion()
            + ".zip";
        MessageDigest digest = MessageDigest.getInstance("SHA-512");
        UserException e = expectThrows(
            UserException.class,
            () -> assertInstallPluginFromUrl(
                "analysis-icu",
                null,
                url,
                null,
                false,
                ".sha512",
                checksumAndString(digest, "  repository-s3-" + Build.CURRENT.getQualifiedVersion() + ".zip"),
                null,
                (b, p) -> null
            )
        );
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e, hasToString(matches("checksum file at \\[.*\\] is not for this plugin")));
    }

    public void testInvalidShaFileContainingExtraLine() throws Exception {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-"
            + Build.CURRENT.getQualifiedVersion()
            + ".zip";
        MessageDigest digest = MessageDigest.getInstance("SHA-512");
        UserException e = expectThrows(
            UserException.class,
            () -> assertInstallPluginFromUrl(
                "analysis-icu",
                null,
                url,
                null,
                false,
                ".sha512",
                checksumAndString(digest, "  analysis-icu-" + Build.CURRENT.getQualifiedVersion() + ".zip\nfoobar"),
                null,
                (b, p) -> null
            )
        );
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("Invalid checksum file"));
    }

    public void testSha512Mismatch() {
        String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-"
            + Build.CURRENT.getQualifiedVersion()
            + ".zip";
        UserException e = expectThrows(
            UserException.class,
            () -> assertInstallPluginFromUrl(
                "analysis-icu",
                null,
                url,
                null,
                false,
                ".sha512",
                bytes -> "foobar  analysis-icu-" + Build.CURRENT.getQualifiedVersion() + ".zip",
                null,
                (b, p) -> null
            )
        );
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("SHA-512 mismatch, expected foobar"));
    }

    public void testSha1Mismatch() {
        String url = "https://repo1.maven.org/maven2/mygroup/myplugin/1.0.0/myplugin-1.0.0.zip";
        UserException e = expectThrows(
            UserException.class,
            () -> assertInstallPluginFromUrl(
                "myplugin",
                "mygroup:myplugin:1.0.0",
                url,
                null,
                false,
                ".sha1",
                bytes -> "foobar",
                null,
                (b, p) -> null
            )
        );
        assertEquals(ExitCodes.IO_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("SHA-1 mismatch, expected foobar"));
    }

    public void testPublicKeyIdMismatchToExpectedPublicKeyId() throws Exception {
        final String icu = "analysis-icu";
        final String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/"
            + icu
            + "-"
            + Build.CURRENT.getQualifiedVersion()
            + ".zip";
        final MessageDigest digest = MessageDigest.getInstance("SHA-512");
        /*
         * To setup a situation where the expected public key ID does not match the public key ID used for signing, we generate a new public
         * key at the moment of signing (see the signature invocation). Note that this key will not match the key that we push down to the
         * install plugin command.
         */
        final PGPSecretKey signingKey = newSecretKey(); // the actual key used for signing
        final String actualID = Long.toHexString(signingKey.getKeyID()).toUpperCase(Locale.ROOT);
        final BiFunction<byte[], PGPSecretKey, String> signature = (b, p) -> signature(b, signingKey);
        final PGPSecretKey verifyingKey = newSecretKey(); // the expected key used for signing
        final String expectedID = Long.toHexString(verifyingKey.getKeyID()).toUpperCase(Locale.ROOT);
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> assertInstallPluginFromUrl(
                icu,
                null,
                url,
                null,
                false,
                ".sha512",
                checksumAndFilename(digest, url),
                verifyingKey,
                signature
            )
        );
        assertThat(e.getMessage(), containsString("key id [" + actualID + "] does not match expected key id [" + expectedID + "]"));
    }

    public void testFailedSignatureVerification() throws Exception {
        final String icu = "analysis-icu";
        final String url = "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/"
            + icu
            + "-"
            + Build.CURRENT.getQualifiedVersion()
            + ".zip";
        final MessageDigest digest = MessageDigest.getInstance("SHA-512");
        /*
         * To setup a situation where signature verification fails, we will mutate the input byte array by modifying a single byte to some
         * random byte value other than the actual value. This is enough to change the signature and cause verification to intentionally
         * fail.
         */
        final BiFunction<byte[], PGPSecretKey, String> signature = (b, p) -> {
            final byte[] bytes = Arrays.copyOf(b, b.length);
            bytes[0] = randomValueOtherThan(b[0], ESTestCase::randomByte);
            return signature(bytes, p);
        };
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> assertInstallPluginFromUrl(
                icu,
                null,
                url,
                null,
                false,
                ".sha512",
                checksumAndFilename(digest, url),
                newSecretKey(),
                signature
            )
        );
        assertThat(e.getMessage(), containsString("signature verification for [" + url + "] failed"));
    }

    public PGPSecretKey newSecretKey() throws NoSuchAlgorithmException, PGPException {
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final KeyPair pair = kpg.generateKeyPair();
        final PGPDigestCalculator sha1Calc = new JcaPGPDigestCalculatorProviderBuilder().build().get(HashAlgorithmTags.SHA1);
        final PGPKeyPair pkp = new JcaPGPKeyPair(PGPPublicKey.RSA_GENERAL, pair, new Date());
        return new PGPSecretKey(
            PGPSignature.DEFAULT_CERTIFICATION,
            pkp,
            "example@example.com",
            sha1Calc,
            null,
            null,
            new JcaPGPContentSignerBuilder(pkp.getPublicKey().getAlgorithm(), HashAlgorithmTags.SHA256),
            new JcePBESecretKeyEncryptorBuilder(PGPEncryptedData.AES_192, sha1Calc).setProvider(new BouncyCastleFipsProvider())
                .build("passphrase".toCharArray())
        );
    }

    private Function<byte[], String> checksum(final MessageDigest digest) {
        return checksumAndString(digest, "");
    }

    private Function<byte[], String> checksumAndFilename(final MessageDigest digest, final String url) {
        final String[] segments = URI.create(url).getPath().split("/");
        return checksumAndString(digest, "  " + segments[segments.length - 1]);
    }

    private Function<byte[], String> checksumAndString(final MessageDigest digest, final String s) {
        return bytes -> MessageDigests.toHexString(digest.digest(bytes)) + s;
    }

    private String signature(final byte[] bytes, final PGPSecretKey secretKey) {
        try {
            final PGPPrivateKey privateKey = secretKey.extractPrivateKey(
                new JcePBESecretKeyDecryptorBuilder(new JcaPGPDigestCalculatorProviderBuilder().build()).build("passphrase".toCharArray())
            );
            final PGPSignatureGenerator generator = new PGPSignatureGenerator(
                new JcaPGPContentSignerBuilder(privateKey.getPublicKeyPacket().getAlgorithm(), HashAlgorithmTags.SHA512)
            );
            generator.init(PGPSignature.BINARY_DOCUMENT, privateKey);
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (
                BCPGOutputStream pout = new BCPGOutputStream(new ArmoredOutputStream(output));
                InputStream is = new ByteArrayInputStream(bytes)
            ) {
                final byte[] buffer = new byte[1024];
                int read;
                while ((read = is.read(buffer)) != -1) {
                    generator.update(buffer, 0, read);
                }
                generator.generate().encode(pout);
            }
            return new String(output.toByteArray(), "UTF-8");
        } catch (IOException | PGPException e) {
            throw new RuntimeException(e);
        }
    }

    // checks the plugin requires a policy confirmation, and does not install when that is rejected by the user
    // the plugin is installed after this method completes
    private void assertPolicyConfirmation(Tuple<Path, Environment> pathEnvironmentTuple, InstallablePlugin pluginZip, String... warnings)
        throws Exception {
        for (int i = 0; i < warnings.length; ++i) {
            String warning = warnings[i];
            for (int j = 0; j < i; ++j) {
                terminal.addTextInput("y"); // accept warnings we have already tested
            }
            // default answer, does not install
            terminal.addTextInput("");
            UserException e = expectThrows(UserException.class, () -> installPlugin(pluginZip));
            assertThat(e.getMessage(), containsString("installation aborted by user"));

            assertThat(terminal.getErrorOutput(), containsString("WARNING: " + warning));
            try (Stream<Path> fileStream = Files.list(pathEnvironmentTuple.v2().pluginsFile())) {
                assertThat(fileStream.collect(Collectors.toList()), empty());
            }

            // explicitly do not install
            terminal.reset();
            for (int j = 0; j < i; ++j) {
                terminal.addTextInput("y"); // accept warnings we have already tested
            }
            terminal.addTextInput("n");
            e = expectThrows(UserException.class, () -> installPlugin(pluginZip));
            assertThat(e.getMessage(), containsString("installation aborted by user"));
            assertThat(terminal.getErrorOutput(), containsString("WARNING: " + warning));
            try (Stream<Path> fileStream = Files.list(pathEnvironmentTuple.v2().pluginsFile())) {
                assertThat(fileStream.collect(Collectors.toList()), empty());
            }
        }

        // allow installation
        terminal.reset();
        for (int j = 0; j < warnings.length; ++j) {
            terminal.addTextInput("y");
        }
        installPlugin(pluginZip);
        for (String warning : warnings) {
            assertThat(terminal.getErrorOutput(), containsString("WARNING: " + warning));
        }
    }

    public void testPolicyConfirmation() throws Exception {
        writePluginSecurityPolicy(pluginDir, "createClassLoader", "setFactory");
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir);

        assertPolicyConfirmation(env, pluginZip, "plugin requires additional permissions");
        assertPlugin("fake", pluginDir, env.v2());
    }

    public void testPluginWithNativeController() throws Exception {
        InstallablePlugin pluginZip = createPluginZip("fake", pluginDir, "has.native.controller", "true");

        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> installPlugin(pluginZip));
        assertThat(e.getMessage(), containsString("plugins can not have native controllers"));
    }

    public void testMultipleJars() throws Exception {
        writeJar(pluginDir.resolve("dep1.jar"), "Dep1");
        writeJar(pluginDir.resolve("dep2.jar"), "Dep2");
        InstallablePlugin pluginZip = createPluginZip("fake-with-deps", pluginDir);
        installPlugin(pluginZip);
        assertPlugin("fake-with-deps", pluginDir, env.v2());
    }
}
