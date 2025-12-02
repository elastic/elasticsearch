/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.cli.Terminal;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * A PGP signature verifier that delegates to Bouncy Castle implementation loaded in an isolated classloader.
 */
public class IsolatedBcPgpSignatureVerifier implements PgpSignatureVerifier {

    private final Terminal terminal;

    public IsolatedBcPgpSignatureVerifier(Terminal terminal) {
        this.terminal = terminal;
    }

    @Override
    public void verifySignature(Path libDir, Path zip, String urlString, InputStream ascInputStream) throws IOException {
        try (ParentLastUrlClassLoader classLoader = classLoader(libDir)) {
            Class<?> clazz = Class.forName("org.elasticsearch.plugins.cli.BcPgpSignatureVerifier", true, classLoader);
            Constructor<?> constructor = clazz.getDeclaredConstructor(Terminal.class);
            Method method = clazz.getMethod("verifySignature", Path.class, Path.class, String.class, InputStream.class);
            method.invoke(constructor.newInstance(terminal), libDir, zip, urlString, ascInputStream);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static ParentLastUrlClassLoader classLoader(Path libDir) throws IOException {
        return new ParentLastUrlClassLoader(
            urls(libDir),
            IsolatedBcPgpSignatureVerifier.class.getClassLoader(),
            name -> name.startsWith("org.bouncycastle.") || name.startsWith("org.elasticsearch.plugins.cli.")
        );
    }

    private static URL[] urls(Path libDir) throws IOException {
        List<URL> urls = new ArrayList<>();
        addJarFileUrls(urls, libDir); // Other plugin-cli JARs for BcPgpSignatureVerifier
        addJarFileUrls(urls, libDir.resolve("bcfips")); // Bouncy Castle FIPS jars
        return urls.toArray(URL[]::new);
    }

    private static void addJarFileUrls(List<URL> urls, Path dir) throws IOException {
        try (Stream<Path> jarFiles = Files.list(dir)) {
            List<Path> jars = jarFiles.filter(p -> p.toString().endsWith(".jar")).toList();
            for (Path jar : jars) {
                urls.add(jar.toUri().toURL());
            }
        }
    }

}
