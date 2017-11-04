/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@LuceneTestCase.SuppressFileSystems("*")
public class InstallXPackExtensionCommandTests extends ESTestCase {

    Path home;
    Environment env;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        home = createTempDir();
        Files.createDirectories(home.resolve("org/elasticsearch/xpack/extensions").resolve("xpack").resolve("extensions"));
        env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", home.toString()).build());
    }

    /**
     * creates a fake jar file with empty class files
     */
    static void writeJar(Path jar, String... classes) throws IOException {
        try (ZipOutputStream stream = new ZipOutputStream(Files.newOutputStream(jar))) {
            for (String clazz : classes) {
                stream.putNextEntry(new ZipEntry(clazz + ".class")); // no package names, just support simple classes
            }
        }
    }

    static String writeZip(Path structure) throws IOException {
        Path zip = createTempDir().resolve(structure.getFileName() + ".zip");
        try (ZipOutputStream stream = new ZipOutputStream(Files.newOutputStream(zip))) {
            Files.walkFileTree(structure, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String target = structure.relativize(file).toString();
                    stream.putNextEntry(new ZipEntry(target));
                    Files.copy(file, stream);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        return zip.toUri().toURL().toString();
    }

    /**
     * creates an extension .zip and returns the url for testing
     */
    static String createExtension(String name, Path structure) throws IOException {
        XPackExtensionTestUtil.writeProperties(structure,
                "description", "fake desc",
                "name", name,
                "version", "1.0",
                "xpack.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "FakeExtension");
        writeJar(structure.resolve("extension.jar"), "FakeExtension");
        return writeZip(structure);
    }

    static MockTerminal installExtension(String extensionUrl, Path home) throws Exception {
        Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", home).build());
        MockTerminal terminal = new MockTerminal();
        new InstallXPackExtensionCommand().execute(terminal, extensionUrl, true, env);
        return terminal;
    }

    void assertExtension(String name, Environment env) throws IOException {
        Path got = env.pluginsFile().resolve("x-pack").resolve("extensions").resolve(name);
        assertTrue("dir " + name + " exists", Files.exists(got));
        assertTrue("jar was copied", Files.exists(got.resolve("extension.jar")));
        assertInstallCleaned(env);
    }

    void assertInstallCleaned(Environment env) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(env.pluginsFile().resolve("x-pack").resolve("extensions"))) {
            for (Path file : stream) {
                if (file.getFileName().toString().startsWith(".installing")) {
                    fail("Installation dir still exists, " + file);
                }
            }
        }
    }

    public void testSomethingWorks() throws Exception {
        Path extDir = createTempDir();
        String extZip = createExtension("fake", extDir);
        installExtension(extZip, home);
        assertExtension("fake", env);
    }

    public void testSpaceInUrl() throws Exception {
        Path extDir = createTempDir();
        String extZip = createExtension("fake", extDir);
        Path extZipWithSpaces = createTempFile("foo bar", ".zip");
        try (InputStream in = FileSystemUtils.openFileURLStream(new URL(extZip))) {
            Files.copy(in, extZipWithSpaces, StandardCopyOption.REPLACE_EXISTING);
        }
        installExtension(extZipWithSpaces.toUri().toURL().toString(), home);
        assertExtension("fake", env);
    }

    public void testMalformedUrlNotMaven() throws Exception {
        // has two colons, so it appears similar to maven coordinates
        MalformedURLException e = expectThrows(MalformedURLException.class, () -> {
            installExtension("://host:1234", home);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("no protocol"));
    }

    public void testJarHell() throws Exception {
        Path extDir = createTempDir();
        writeJar(extDir.resolve("other.jar"), "FakeExtension");
        String extZip = createExtension("fake", extDir); // adds extension.jar with FakeExtension
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> installExtension(extZip, home));
        assertTrue(e.getMessage(), e.getMessage().contains("jar hell"));
        assertInstallCleaned(env);
    }

    public void testIsolatedExtension() throws Exception {
        // these both share the same FakeExtension class
        Path extDir1 = createTempDir();
        String extZip1 = createExtension("fake1", extDir1);
        installExtension(extZip1, home);
        Path extDir2 = createTempDir();
        String extZip2 = createExtension("fake2", extDir2);
        installExtension(extZip2, home);
        assertExtension("fake1", env);
        assertExtension("fake2", env);
    }

    public void testExistingExtension() throws Exception {
        String extZip = createExtension("fake", createTempDir());
        installExtension(extZip, home);
        UserException e = expectThrows(UserException.class, () -> installExtension(extZip, home));
        assertTrue(e.getMessage(), e.getMessage().contains("already exists"));
        assertInstallCleaned(env);
    }

    public void testMissingDescriptor() throws Exception {
        Path extDir = createTempDir();
        Files.createFile(extDir.resolve("fake.yml"));
        String extZip = writeZip(extDir);
        NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> installExtension(extZip, home));
        assertTrue(e.getMessage(), e.getMessage().contains("x-pack-extension-descriptor.properties"));
        assertInstallCleaned(env);
    }
}
