/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An interface for command line tools to be instantiated and run.
 */
public interface CliToolProvider {

    /**
     * Returns the name of the tool provider.
     * This must be unique across all tool providers.
     */
    String name();

    /**
     * Constructs the CLI instance to be run.
     */
    Command create();

    /**
     * Loads a tool provider from the Elasticsearch distribution.
     *
     * @param sysprops the system properties of the CLI process
     * @param toolname the name of the tool to load
     * @param libs the library directories to load, relative to the Elasticsearch homedir
     * @return the instance of the loaded tool
     * @throws AssertionError if the given toolname cannot be found or there are more than one tools found with the same name
     */
    static CliToolProvider load(Map<String, String> sysprops, String toolname, String libs) {
        Path homeDir = Paths.get(sysprops.get("es.path.home")).toAbsolutePath();
        final ClassLoader cliLoader;
        if (libs.isBlank()) {
            cliLoader = ClassLoader.getSystemClassLoader();
        } else {
            List<Path> libsToLoad = Stream.of(libs.split(",")).map(homeDir::resolve).toList();
            cliLoader = loadJars(libsToLoad);
        }

        ServiceLoader<CliToolProvider> toolFinder = ServiceLoader.load(CliToolProvider.class, cliLoader);
        List<CliToolProvider> tools = StreamSupport.stream(toolFinder.spliterator(), false).filter(p -> p.name().equals(toolname)).toList();
        if (tools.size() > 1) {
            String names = tools.stream().map(t -> t.getClass().getName()).collect(Collectors.joining(", "));
            throw new AssertionError("Multiple ToolProviders found with name [" + toolname + "]: " + names);
        }
        if (tools.size() == 0) {
            var names = StreamSupport.stream(toolFinder.spliterator(), false).map(CliToolProvider::name).toList();
            throw new AssertionError("CliToolProvider [" + toolname + "] not found, available names are " + names);
        }
        return tools.get(0);
    }

    private static ClassLoader loadJars(List<Path> dirs) {
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
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return URLClassLoader.newInstance(urls.toArray(URL[]::new));
    }
}
