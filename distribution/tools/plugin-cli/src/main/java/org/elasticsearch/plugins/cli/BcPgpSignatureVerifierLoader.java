/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.core.PathUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A PGP signature verifier that delegates to Bouncy Castle implementation loaded in an isolated classloader.
 */
class BcPgpSignatureVerifierLoader implements Supplier<BiConsumer<Path, InputStream>> {

    private final String urlString;
    private final Consumer<String> terminal;

    BcPgpSignatureVerifierLoader(String urlString, Consumer<String> terminal) {
        this.urlString = urlString;
        this.terminal = terminal;
    }

    @Override
    public BiConsumer<Path, InputStream> get() {
        return this::verifySignature;
    }

    @SuppressWarnings("unchecked")
    public void verifySignature(Path zip, InputStream ascInputStream) {
        try (URLClassLoader classLoader = classLoader()) {
            Class<?> clazz = Class.forName("org.elasticsearch.plugins.cli.BcPgpSignatureVerifier", true, classLoader);
            Constructor<?> constructor = clazz.getConstructor(String.class, Consumer.class);
            BiConsumer<Path, InputStream> bc = (BiConsumer<Path, InputStream>) constructor.newInstance(urlString, terminal);
            bc.accept(zip, ascInputStream);
        } catch (ReflectiveOperationException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static URLClassLoader classLoader() throws IOException {
        return new URLClassLoader(urls(), ClassLoader.getPlatformClassLoader());
    }

    private static URL[] urls() throws IOException {
        String esHome = System.getProperty("es.path.home");
        if (esHome == null) {
            throw new IllegalStateException("es.path.home system property is required");
        }
        String cliLibs = System.getProperty("cli.libs");
        if (cliLibs == null || cliLibs.isBlank()) {
            throw new IllegalStateException("cli.libs system property is required");
        }
        return urls(esHome, cliLibs);
    }

    private static URL[] urls(String esHome, String cliLibs) throws IOException {
        Path homeDir = PathUtils.get(esHome);
        List<URL> urls = new ArrayList<>();
        for (String lib : cliLibs.split(",")) {
            Path libDir = homeDir.resolve(lib);
            try (Stream<Path> jarFiles = Files.list(libDir)) {
                for (Path p : jarFiles.filter(p -> p.getFileName().toString().endsWith(".jar")).toList()) {
                    urls.add(p.toUri().toURL());
                }
            }
        }
        return urls.toArray(URL[]::new);
    }

}
