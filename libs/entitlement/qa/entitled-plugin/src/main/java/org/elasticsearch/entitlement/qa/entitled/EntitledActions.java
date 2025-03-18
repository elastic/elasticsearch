/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.entitled;

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.UserPrincipal;
import java.security.SecureRandom;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

@SuppressForbidden(reason = "Exposes forbidden APIs for testing purposes")
public final class EntitledActions {
    private EntitledActions() {}

    private static final SecureRandom random = new SecureRandom();

    private static final Path testRootDir = Paths.get(System.getProperty("es.entitlements.testdir"));

    private static Path readDir() {
        return testRootDir.resolve("read_dir");
    }

    private static Path readWriteDir() {
        return testRootDir.resolve("read_write_dir");
    }

    public static UserPrincipal getFileOwner(Path path) throws IOException {
        return Files.getOwner(path);
    }

    public static void createFile(Path path) throws IOException {
        Files.createFile(path);
    }

    public static Path createTempFileForRead() throws IOException {
        return Files.createFile(readDir().resolve("entitlements-" + random.nextLong() + ".tmp"));
    }

    public static Path createTempFileForWrite() throws IOException {
        return Files.createFile(readWriteDir().resolve("entitlements-" + random.nextLong() + ".tmp"));
    }

    public static Path createTempDirectoryForWrite() throws IOException {
        return Files.createDirectory(readWriteDir().resolve("entitlements-dir-" + random.nextLong()));
    }

    public static Path createTempSymbolicLink() throws IOException {
        return createTempSymbolicLink(readWriteDir());
    }

    public static Path createTempSymbolicLink(Path target) throws IOException {
        return Files.createSymbolicLink(readDir().resolve("entitlements-link-" + random.nextLong()), target);
    }

    public static Path pathToRealPath(Path path) throws IOException {
        return path.toRealPath();
    }

    public static Path createK8sLikeMount() throws IOException {
        Path baseDir = readDir().resolve("k8s");
        var versionedDir = Files.createDirectories(baseDir.resolve("..version"));
        var actualFileMount = Files.createFile(versionedDir.resolve("mount-" + random.nextLong() + ".tmp"));

        var dataDir = Files.createSymbolicLink(baseDir.resolve("..data"), versionedDir.getFileName());
        // mount-0.tmp -> ..data/mount-0.tmp -> ..version/mount-0.tmp
        return Files.createSymbolicLink(
            baseDir.resolve(actualFileMount.getFileName()),
            dataDir.getFileName().resolve(actualFileMount.getFileName())
        );
    }

    public static URLConnection createHttpURLConnection() throws IOException {
        return URI.create("http://127.0.0.1:12345/").toURL().openConnection();
    }

    public static URLConnection createHttpsURLConnection() throws IOException {
        return URI.create("https://127.0.0.1:12345/").toURL().openConnection();
    }

    public static URLConnection createFtpURLConnection() throws IOException {
        return URI.create("ftp://127.0.0.1:12345/").toURL().openConnection();
    }

    public static URLConnection createFileURLConnection() throws IOException {
        var fileUrl = createTempFileForWrite().toUri().toURL();
        return fileUrl.openConnection();
    }

    public static URLConnection createMailToURLConnection() throws URISyntaxException, IOException {
        return new URI("mailto", "email@example.com", null).toURL().openConnection();
    }

    public static Path createJar(Path dir, String name, Manifest manifest, String... files) throws IOException {
        Path jarpath = dir.resolve(name);
        try (var os = Files.newOutputStream(jarpath, StandardOpenOption.CREATE); var out = new JarOutputStream(os, manifest)) {
            for (String file : files) {
                out.putNextEntry(new JarEntry(file));
            }
        }
        return jarpath;
    }

    public static URLConnection createJarURLConnection() throws IOException {
        var manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        var tmpJarFile = createJar(readWriteDir(), "entitlements-" + random.nextLong() + ".jar", manifest, "a", "b");
        var jarFileUrl = tmpJarFile.toUri().toURL();
        var jarUrl = URI.create("jar:" + jarFileUrl + "!/a").toURL();
        return jarUrl.openConnection();
    }
}
