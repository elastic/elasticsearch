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
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface ToolProvider {

    // TODO: merge launcher and cli lib, move to subdir in distro, make compileOnly dep in server
    String name();

    // TODO: this is temporary
    Command create();

    static ToolProvider loadTool(String toolname, String libs) throws IOException {
        // the ES homedir is always our working dir
        Path homeDir = Paths.get("").toAbsolutePath();
        final ClassLoader cliLoader;
        if (libs != null) {
            List<Path> libsToLoad = Stream.of(libs.split(",")).map(homeDir::resolve).toList();
            cliLoader = loadJars(libsToLoad);
        } else {
            cliLoader = ClassLoader.getSystemClassLoader();
        }

        ServiceLoader<ToolProvider> toolFinder = ServiceLoader.load(ToolProvider.class, cliLoader);
        // TODO: assert only one tool is found?
        return StreamSupport.stream(toolFinder.spliterator(), false)
            .filter(p -> p.name().equals(toolname))
            .findFirst()
            .orElseThrow(() -> new AssertionError("ToolProvider [" + toolname + "] not found"));
    }

    private static ClassLoader loadJars(List<Path> dirs) throws IOException {
        final List<URL> urls = new ArrayList<>();
        for (var dir : dirs) {
            try (Stream<Path> jarFiles = Files.list(dir)) {
                jarFiles.filter(p -> p.getFileName().toString().endsWith(".jar")).map(p -> {
                    try {
                        return p.toUri().toURL();
                    } catch (MalformedURLException e) {
                        throw new AssertionError(e);
                    }
                }).forEach(urls::add);
            }
        }
        return URLClassLoader.newInstance(urls.toArray(URL[]::new));
    }
}
