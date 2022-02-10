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
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A provider locator for finding the {@link XContentProvider}.
 *
 * <p> Provider implementations are located as follows:
 * <ol>
 *   <li> First the <i>provider specific</i> path is found, </li>
 *   <li> then the provider is loaded from the artifacts located at that path. </li>
 * </ol>
 *
 * <p> The provider specific path is found by searching for the <i>root providers</i> path, then resolving the specific provider
 * type against that path, <i>x-content</i> in this case. The root providers' path is an entry on the class path referring to a directory
 * in ending with "providers". Within the distribution this will be <i>lib/providers</i>, but may vary when running tests.
 *
 * <p> The artifacts that constitute the provider implementation (the impl jar and its dependencies) are explicitly named in the
 * provider-jars.txt resource file. Each name is resolved against the provider specific path to locate the actual on disk jar artifact. The
 * collection of these artifacts constitute the provider implementation.
 */
public final class ProviderLocator {

    /**
     * Returns the provider instance.
     */
    public static final XContentProvider INSTANCE = provider();

    @SuppressForbidden(reason = "path separator")
    private static String pathSeparator() {
        return File.pathSeparator;
    }

    private static Path providerPath() {
        // TODO: resolve x-content in stream, then expect one
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

    private static URL[] gatherUrls(Path providerPath) throws IOException {
        final InputStream is = ProviderLocator.class.getResourceAsStream("provider-jars.txt");
        if (is == null) {
            throw new IllegalStateException("missing x-content provider jars list");
        }

        final List<Path> paths;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            paths = reader.lines().map(providerPath::resolve).collect(Collectors.toList());
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
