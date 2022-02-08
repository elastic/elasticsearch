/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.internal;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.spi.XContentProvider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

public final class ProviderLocator {

    /** List of known expected impl jar and dependency file name prefixes. */
    private static final List<String> JAR_PREFIXES = List.of("jackson", "snakeyaml", "x-content-impl");

    public static final XContentProvider INSTANCE = provider();

    @SuppressForbidden(reason = "path separator")
    private static String pathSeparator() {
        return File.pathSeparator;
    }

    private static Path providerPath() {
        String classpath = System.getProperty("java.class.path");
        List<Path> path = Arrays.stream(classpath.split(ProviderLocator.pathSeparator()))
            .filter(s -> s.endsWith("providers"))
            .map(Path::of)
            .filter(Files::isDirectory)
            .toList();
        if (path.size() != 1) {
            throw new RuntimeException("Expected one provider path, found:" + path);
        }

        return checkPathExists(path.get(0).resolve("x-content"));
    }

    private static Path checkPathExists(Path path) {
        if (Files.notExists(path) || Files.isDirectory(path) == false) {
            throw new UncheckedIOException(new FileNotFoundException(path.toString()));
        }
        return path;
    }

    private static XContentProvider provider() {
        try {
            PrivilegedExceptionAction<XContentProvider> pa = () -> loadAsNonModule(gatherUrls(providerPath()));
            return AccessController.doPrivileged(pa);
        } catch (PrivilegedActionException e) {
            throw new UncheckedIOException((IOException) e.getCause());
        }
    }

    private static XContentProvider loadAsNonModule(URL[] urls) {
        URLClassLoader loader = URLClassLoader.newInstance(urls, XContentProvider.class.getClassLoader());
        ServiceLoader<XContentProvider> sl = ServiceLoader.load(XContentProvider.class, loader);
        return sl.findFirst().orElseThrow(() -> new RuntimeException("cannot locate x-content provider"));
    }

    /** Returns true if the path refers to a jar with one of the known expected names. */
    static boolean isProviderJar(Path path) {
        if (Files.isDirectory(path) == false) {
            String name = path.getFileName().toString();
            if (name.endsWith(".jar") && JAR_PREFIXES.stream().anyMatch(name::startsWith)) {
                return true;
            }
        }
        return false;
    }

    private static URL[] gatherUrls(Path dir) throws IOException {
        final List<Path> paths;
        try (InputStream is = ProviderLocator.class.getResourceAsStream("provider-jars.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            paths = reader.lines().map(dir::resolve).collect(Collectors.toList());
            //return loadAsNonModule(reader.lines().map(ProviderLocator.class::getResource).toArray(URL[]::new));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Set<URL> urls = new LinkedHashSet<>();
        for (Path path : paths) {
            URL url = path.toRealPath().toUri().toURL();
            if (urls.add(url) == false) {
                throw new IllegalStateException("duplicate codebase: " + url);
            }
        }
        return urls.toArray(URL[]::new);
    }
}
