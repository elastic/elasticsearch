/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin.scanner;

import org.objectweb.asm.ClassReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

/**
 * A utility class containing methods to create streams of ASM's ClassReader
 *
 * @see ClassReader
 */
public class ClassReaders {
    // TODO javadoc on closing a stream

    public static Stream<ClassReader> ofDirWithJars(String path)  {
        if (path == null) {
            return Stream.empty();
        }
        Path dir = Paths.get(path);
        try {
            return ofPaths(Files.list(dir));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Stream<ClassReader> ofPaths(Stream<Path> list) {
        return list.filter(Files::exists).flatMap(p -> {
            if (p.toString().endsWith(".jar")) {
                return classesInJar(p);
            } else {
                return classesInPath(p);
            }
        });
    }

    private static final String MODULE_INFO = "module-info.class";

    private static Stream<ClassReader> classesInJar(Path jar) {
        // try {
        // FileSystem jarFs = FileSystems.newFileSystem(jar);
        // Path root = jarFs.getPath("/");

        try {
            JarFile jf = new JarFile(jar.toFile(), true, ZipFile.OPEN_READ, Runtime.version());

            Stream<ClassReader> classReaderStream = jf.versionedStream()
                .filter(e -> e.getName().endsWith(".class") && e.getName().equals(MODULE_INFO) == false)
                .map(e -> {
                    try (InputStream is = jf.getInputStream(e)) {
                        byte[] classBytes = is.readAllBytes();
                        return new ClassReader(classBytes);
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                });
            return classReaderStream;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Stream.empty();
    }

    private static Stream<ClassReader> classesInPath(Path root) {
        try { // todo what to do on exception
            Stream<Path> stream = Files.walk(root);
            return stream.filter(p -> p.toString().endsWith(".class"))
                .filter(p -> p.toString().endsWith("module-info.class") == false)
                .map(p -> {
                    try (InputStream is = Files.newInputStream(p)) {
                        byte[] classBytes = is.readAllBytes();
                        return new ClassReader(classBytes);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
        } catch (IOException e) {
            // e.printStackTrace();
        }
        return Stream.empty();
    }

    // Duplication from PluginsService::uncheckedToURI
    public static final URI uncheckedToURI(URL url) {
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new AssertionError(new IOException(e));
        }
    }
}
