/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

/**
 * Dynamically loads an "AnsiPrintStream" from the JANSI library on a separate class loader (so that the server classpath
 * does not need to include jansi.jar
 */
public class ConsoleLoader {

    private static final String CONSOLE_LOADER_CLASS = "org.elasticsearch.io.ansi.AnsiConsoleLoader";

    @SuppressWarnings("unchecked")
    public static PrintStream loadConsole(Environment env) {
        try {
            final Class<? extends Supplier<PrintStream>> cls = (Class<? extends Supplier<PrintStream>>) buildClassLoader(env).loadClass(
                CONSOLE_LOADER_CLASS
            );
            final Constructor<? extends Supplier<PrintStream>> constructor = cls.getConstructor();
            Supplier<PrintStream> supplier = constructor.newInstance();
            return supplier.get();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to load ANSI console", e);
        }
    }

    private static ClassLoader buildClassLoader(Environment env) {
        final Path libDir = env.libFile().resolve("tools").resolve("ansi-console");

        try {
            final URL[] urls = Files.list(libDir)
                .filter(each -> each.getFileName().toString().endsWith(".jar"))
                .map(ConsoleLoader::pathToURL)
                .toArray(URL[]::new);

            return URLClassLoader.newInstance(urls, ConsoleLoader.class.getClassLoader());
        } catch (IOException e) {
            throw new RuntimeException("Failed to list jars in [" + libDir + "]: " + e.getMessage(), e);
        }
    }

    private static URL pathToURL(Path path) {
        try {
            return path.toUri().toURL();
        } catch (MalformedURLException e) {
            // Shouldn't happen, but have to handle the exception
            throw new RuntimeException("Failed to convert path [" + path + "] to URL", e);
        }
    }
}
