/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.launcher;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.ToolProvider;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
        configureLoggingWithoutConfig();

        // TODO: change signature of Command to take in sysprops and env
        Map<String, String> sysprops = convertPropertiesToMap(System.getProperties());
        Map<String, String> env = new HashMap<>(System.getenv());

        Path homeDir = Paths.get("").toAbsolutePath();
        String toolname = env.get("LAUNCHER_TOOLNAME");
        String libs = env.get("LAUNCHER_LIBS");

        System.out.println("Running ES cli");
        System.out.println("ES_HOME=" + homeDir);
        System.out.println("tool: " + toolname);
        System.out.println("libs: " + libs);
        System.out.println("args: " + Arrays.asList(args));

        final ClassLoader cliLoader;
        if (libs != null) {
            List<Path> libsToLoad = Stream.of(libs.split(",")).map(homeDir::resolve).toList();
            cliLoader = loadJars(libsToLoad);
        } else {
            cliLoader = ClassLoader.getSystemClassLoader();
        }
        ServiceLoader<ToolProvider> toolFinder = ServiceLoader.load(ToolProvider.class, cliLoader);
        // TODO: assert only one tool is found?
        ToolProvider tool = StreamSupport.stream(toolFinder.spliterator(), false)
            .filter(p -> p.name().equals(toolname))
            .findFirst()
            .orElseThrow(() -> new AssertionError("ToolProvider [" + toolname + "] not found"));
        Command toolCommand = tool.create();
        System.exit(toolCommand.main(args, Terminal.DEFAULT));
    }

    private static Map<String, String> convertPropertiesToMap(Properties properties) {
        return properties.entrySet().stream().collect(
            Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
    }

    private static ClassLoader loadJars(List<Path> dirs) throws IOException {
        final List<URL> urls = new ArrayList<>();
        for (var dir : dirs) {
            try (Stream<Path> jarFiles = Files.list(dir)) {
                jarFiles.filter(JAR_PREDICATE).map(MAP_PATH_TO_URL).forEach(urls::add);
            }
        }
        return URLClassLoader.newInstance(urls.toArray(URL[]::new));
    }

    /**
     * Configures logging without Elasticsearch configuration files based on the system property "es.logger.level" only. As such, any
     * logging will be written to the console.
     */
    public static void configureLoggingWithoutConfig() {
        // initialize default for es.logger.level because we will not read the log4j2.properties
        final String loggerLevel = System.getProperty("es.logger.level", Level.INFO.name());
        final Settings settings = Settings.builder().put("logger.level", loggerLevel).build();
        LogConfigurator.configureWithoutConfig(settings);
    }
}
