/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.internal;

import org.elasticsearch.xcontent.spi.XContentProvider;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.ServiceLoader;
import java.util.Set;

public final class ProviderLocator {

    public static final Path PROVIDER_PATH = providerPath();

    public static final XContentProvider INSTANCE = provider();

    private static Path providerPath() {
        PrivilegedAction<String> pa = () -> System.getProperty("es.x.content.provider");
        String prop = AccessController.doPrivileged(pa);
        Path path;
        if (prop != null) {
            path = Path.of(prop);
        } else {
            // distribution
            try {
                URI libPath = asDirectory(XContentProvider.class.getProtectionDomain().getCodeSource().getLocation().toURI());
                path = Path.of(libPath).resolve("xcontent_provider");
            } catch (URISyntaxException | IOException e) {
                throw new RuntimeException(e);
            }
        }
        return checkPathExists(path);
    }

    private static Path checkPathExists(Path path) {
        PrivilegedAction<Path> pa = () -> {
            if (Files.notExists(path) || Files.isDirectory(path) == false) {
                throw new UncheckedIOException(new FileNotFoundException(path.toString()));
            }
            return path;
        };
        return AccessController.doPrivileged(pa);
    }

    private static XContentProvider provider() {
        return loadAsNonModule(PROVIDER_PATH);
    }

    private static XContentProvider loadAsNonModule(Path libPath) {
        try {
            PrivilegedExceptionAction<URL[]> pa = () -> gatherUrls(libPath);
            URL[] urls = AccessController.doPrivileged(pa);
            URLClassLoader loader = URLClassLoader.newInstance(urls, XContentProvider.class.getClassLoader());
            System.out.println("URLS: " + Arrays.stream(urls).toList());
            ServiceLoader<XContentProvider> sl = ServiceLoader.load(XContentProvider.class, loader);
            return sl.findFirst().orElseThrow(() -> new RuntimeException("cannot locate x-content provider in:" + libPath));
        } catch (PrivilegedActionException e) {
            throw new UncheckedIOException((IOException) e.getException());
        }// catch (IOException e) {
        // throw new UncheckedIOException(e);
        // }
    }

    private static URL[] gatherUrls(Path dir) throws IOException {
        Set<URL> urls = new LinkedHashSet<>();
        // gather urls for jar files
        try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(dir, "*.jar")) {
            for (Path jar : jarStream) {
                URL url = jar.toRealPath().toUri().toURL();
                if (urls.add(url) == false) {
                    throw new IllegalStateException("duplicate codebase: " + url);
                }
            }
        }
        return urls.toArray(URL[]::new);
    }

    private static URI asDirectory(URI uri) throws IOException {
        String uriString = uri.toString();
        int idx = uriString.lastIndexOf("/");
        if (idx == -1) {
            throw new IOException("malformed uri: " + uri);
        }
        return URI.create(uriString.substring(0, idx + 1));
    }
}
