/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner;

import org.elasticsearch.core.PathUtils;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

/**
 * A utility class containing methods to create streams of ASM's ClassReader
 *
 * @see ClassReader
 */
public class ClassReaders {
    private static final String MODULE_INFO = "module-info.class";

    /**
     * This method must be used within a try-with-resources statement or similar
     * control structure.
     */
    public static Stream<ClassReader> ofDirWithJars(Path dir) {
        if (dir == null) {
            return Stream.empty();
        }
        try {
            return ofPaths(Files.list(dir));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Stream<ClassReader> ofPaths(Set<URL> classpathFiles) {
        return ofPaths(classpathFiles.stream().map(ClassReaders::toPath));
    }

    private static Path toPath(URL url) {
        try {
            return PathUtils.get(url.toURI());
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * This method must be used within a try-with-resources statement or similar
     * control structure.
     */
    public static Stream<ClassReader> ofPaths(Stream<Path> list) {
        return list.filter(Files::exists).flatMap(p -> {
            if (p.toString().endsWith(".jar")) {
                return classesInJar(p);
            } else {
                return classesInPath(p);
            }
        });
    }

    private static Stream<ClassReader> classesInJar(Path jar) {
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
            throw new UncheckedIOException(e);
        }
    }

    private static Stream<ClassReader> classesInPath(Path root) {
        try {
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
            throw new UncheckedIOException(e);
        }
    }

    public static Stream<ClassReader> ofClassPath() throws IOException {
        String classpath = System.getProperty("java.class.path");
        return ofClassPath(classpath);
    }

    public static Stream<ClassReader> ofClassPath(String classpath) {
        if (classpath != null && classpath.equals("") == false) {// todo when do we set cp to "" ?
            var classpathSeparator = System.getProperty("path.separator");

            String[] pathelements = classpath.split(classpathSeparator);
            return ofPaths(Arrays.stream(pathelements).map(Paths::get));
        }
        return Stream.empty();
    }
}
