/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

class Launcher {

    private static Function<Path, URL> MAP_PATH_TO_URL = p -> {
        try {
            return p.toUri().toURL();
        } catch (MalformedURLException e) {
            throw new AssertionError(e);
        }
    };
    private static Predicate<Path> JAR_PREDICATE = p -> p.getFileName().toString().endsWith(".jar");

    // TODO: don't throw, catch this and give a nice error message
    public static void main(String[] args) throws Exception {
        String toolname = System.getProperty("es.tool");
        Path homeDir = Paths.get("").toAbsolutePath();

        System.out.println("Running ES cli");
        System.out.println("ES_HOME=" + homeDir);
        System.out.println("tool: " + toolname);

        Path libDir = homeDir.resolve("lib");
        ClassLoader serverLoader = loadJars(libDir, ClassLoader.getSystemClassLoader());
        ClassLoader cliLoader = loadJars(libDir.resolve("cli").resolve(toolname), serverLoader);
        ServiceLoader<ToolProvider> toolProvider = ServiceLoader.load(ToolProvider.class, cliLoader);
        ToolProvider tool = toolProvider.findFirst().orElseThrow();
        System.exit(tool.main(args, Terminal.DEFAULT));
    }

    private static ClassLoader loadJars(Path dir, ClassLoader parent) throws IOException {
        final URL[] urls;
        try (Stream<Path> jarFiles = Files.list(dir)) {
            urls = jarFiles.filter(JAR_PREDICATE).map(MAP_PATH_TO_URL).toArray(URL[]::new);
        }
        return URLClassLoader.newInstance(urls, parent);
    }
}
