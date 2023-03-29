/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportClassesScanner {
    private final Logger logger = Logging.getLogger(TransportClassesScanner.class);

    private static final String MODULE_INFO = "module-info.class";
    private Set<String> skipMissingClasses;
    private final String writeableClassName;

    public TransportClassesScanner(Set<String> skipMissingClasses) {
        this(skipMissingClasses, "org.elasticsearch.common.io.stream.Writeable");
    }

    TransportClassesScanner(Set<String> skipMissingClasses, String writeableClassName) {
        this.skipMissingClasses = skipMissingClasses;
        this.writeableClassName = writeableClassName;
    }

    public Set<String> findTransportClasses(Set<File> mainClasses, Set<File> mainClasspath) {
        URL[] allUrls = allClasses(mainClasses, mainClasspath);
        ClassLoader cl = new URLClassLoader(allUrls);

        Class<?> writeableClass = loadClass(cl, writeableClassName);

        Set<String> transportClasses = traverseClassesInRoots(mainClasses).stream()
            .filter(n -> isSubclassOf(n, cl, Set.of(writeableClass)))
            .collect(Collectors.toSet());

        // transportClasses.removeAll(skipMissingClasses);
        return transportClasses;
    }

    private URL[] allClasses(Set<File>... classes) {
        return Arrays.stream(classes).map(this::getURLs).flatMap(Set::stream).toArray(URL[]::new);
    }

    private boolean isSubclassOf(String name, ClassLoader cl, Set<Class<?>> rootClasses) {
        try {
            Class<?> clazz = Class.forName(name, false, cl);
            boolean isPublicConcrete = Modifier.isAbstract(clazz.getModifiers()) == false && Modifier.isPublic(clazz.getModifiers());
            return isPublicConcrete && rootClasses.stream().filter(c -> c.isAssignableFrom(clazz)).findAny().isPresent();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private Set<String> traverseClassesInRoots(Set<File> roots) {
        return roots.stream().filter(File::exists).map(f -> {
            try (var stream = Files.walk(Path.of(f.toString()))) {
                return stream.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".class"))
                    .filter(p -> p.toString().endsWith(MODULE_INFO) == false)
                    .filter(p -> p.toString().startsWith("/META-INF") == false)// skip multi-release files
                    .map(p -> getNameFromFilePath(p.toString()))
                    .collect(Collectors.toList());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).flatMap(List::stream).collect(Collectors.toSet());
    }

    private Set<URL> getURLs(Set<File> files) {
        return files.stream().filter(File::exists).map(f -> {
            try {
                return f.toURI().toURL();
            } catch (MalformedURLException e) {
                return null;
            }
        }).filter(f -> f != null).collect(Collectors.toSet());
    }

    private static String getNameFromFilePath(String file1) {
        String file = file1.split("(java|out)\\/(main|test)\\/(classes/)?")[1];
        String withoutDotClass = file.substring(0, file.lastIndexOf("."));
        return withoutDotClass.replace('/', '.');
    }

    private static Class<?> loadClass(ClassLoader cl, String writeableClassName) {
        Class<?> writeableClass = null;
        try {
            writeableClass = Class.forName(writeableClassName, false, cl);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return writeableClass;
    }
}
