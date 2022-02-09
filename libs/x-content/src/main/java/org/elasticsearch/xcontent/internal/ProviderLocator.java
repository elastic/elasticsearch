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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

public final class ProviderLocator {

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
            return loadAsNonModule(gatherUrls(providerPath()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static XContentProvider loadAsNonModule(URL[] urls) {
        URLClassLoader loader = URLClassLoader.newInstance(urls, XContentProvider.class.getClassLoader());
        ServiceLoader<XContentProvider> sl = ServiceLoader.load(XContentProvider.class, loader);
        return sl.findFirst().orElseThrow(() -> new RuntimeException("cannot locate x-content provider"));
    }

    private static URL[] gatherUrls(Path dir) throws IOException {
        final InputStream is = ProviderLocator.class.getResourceAsStream("provider-jars.txt");
        if (is == null) {
            throw new IllegalStateException("missing x-content provider jars list");
        }

        final List<Path> paths;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            paths = reader.lines().map(dir::resolve).collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        final Set<URL> urls = new LinkedHashSet<>();
        for (Path path : paths) {
            URL url = path.toRealPath().toUri().toURL();
            if (urls.add(url) == false) {
                throw new IllegalStateException("duplicate codebase: " + url);
            }
        }
        return urls.toArray(URL[]::new);
    }
}
