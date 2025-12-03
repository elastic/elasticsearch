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
import java.net.URLClassLoader;
import java.nio.file.Path;

/**
 * A PGP signature verifier that delegates to Bouncy Castle implementation loaded in an isolated classloader.
 */
public class IsolatedBcPgpSignatureVerifier implements PgpSignatureVerifier {

    private final Terminal terminal;

    public IsolatedBcPgpSignatureVerifier(Terminal terminal) {
        this.terminal = terminal;
    }

    @Override
    public void verifySignature(Path zip, String urlString, InputStream ascInputStream) throws IOException {
        try (ParentLastUrlClassLoader classLoader = classLoader()) {
            Class<?> clazz = Class.forName("org.elasticsearch.plugins.cli.BcPgpSignatureVerifier", true, classLoader);
            Constructor<?> constructor = clazz.getConstructor(Terminal.class);
            Method method = clazz.getMethod("verifySignature", Path.class, String.class, InputStream.class);
            method.invoke(constructor.newInstance(terminal), zip, urlString, ascInputStream);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static ParentLastUrlClassLoader classLoader() {
        return new ParentLastUrlClassLoader(
            urls(),
            IsolatedBcPgpSignatureVerifier.class.getClassLoader(),
            name -> name.startsWith("org.bouncycastle.") || name.startsWith("org.elasticsearch.plugins.cli.")
        );
    }

    private static URL[] urls() {
        if (IsolatedBcPgpSignatureVerifier.class.getClassLoader() instanceof URLClassLoader ucl) {
            return ucl.getURLs();
        }
        throw new IllegalStateException("URLClassLoader required");
    }

}
