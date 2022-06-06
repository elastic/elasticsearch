/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * This class looks for plugins whose "type" is "bootstrap". Such plugins
 * will be added to the JVM's boot classpath. The plugins may also define
 * additional JVM options, in order to configure the bootstrap plugins.
 */
public class BootstrapJvmOptions {

    private BootstrapJvmOptions() {}

    public static List<String> bootstrapJvmOptions(Path modules, Path plugins) throws IOException {
        if (Files.isDirectory(modules) == false) {
            throw new IllegalArgumentException("Modules path " + modules + " must be a directory");
        }

        if (Files.isDirectory(plugins) == false) {
            throw new IllegalArgumentException("Plugins path " + plugins + " must be a directory");
        }

        final List<PluginInfo> modulesInfo = getPluginInfo(modules);
        final List<PluginInfo> pluginInfo = getPluginInfo(plugins);

        return generateOptions(modulesInfo, pluginInfo);
    }

    // Find all plugins and return their jars and descriptors.
    private static List<PluginInfo> getPluginInfo(Path plugins) throws IOException {
        final List<PluginInfo> pluginInfo = new ArrayList<>();

        final List<Path> pluginDirs = Files.list(plugins).toList();

        for (Path pluginDir : pluginDirs) {
            if (Files.isDirectory(pluginDir) == false) {
                continue; // validation is done elsewhere in startup. here we just ignore errant files
            }
            final List<String> jarFiles = new ArrayList<>();
            final Properties props = new Properties();

            final List<Path> pluginFiles = Files.list(pluginDir).toList();
            for (Path pluginFile : pluginFiles) {
                final String lowerCaseName = pluginFile.getFileName().toString().toLowerCase(Locale.ROOT);

                if (lowerCaseName.endsWith(".jar")) {
                    jarFiles.add(pluginFile.toString());
                } else if (lowerCaseName.equals("plugin-descriptor.properties")) {
                    try (InputStream stream = Files.newInputStream(pluginFile)) {
                        props.load(stream);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }

            if (props.isEmpty() == false) {
                pluginInfo.add(new PluginInfo(jarFiles, props));
            }
        }

        return pluginInfo;
    }

    // package-private for testing
    static List<String> generateOptions(List<PluginInfo> modulesInfo, List<PluginInfo> pluginInfo) {
        final List<String> bootstrapJars = new ArrayList<>();
        final List<String> extraJavaOptions = new ArrayList<>();

        // Add any additional Java CLI options. This could contain any number of options,
        // but we don't attempt to split them up as all JVM options are concatenated together
        // anyway

        final Consumer<PluginInfo> infoConsumer = info -> {
            final String type = info.properties.getProperty("type", "isolated").toLowerCase(Locale.ROOT);
            if (type.equals("bootstrap")) {
                bootstrapJars.addAll(info.jarFiles);
            }
            final String javaOpts = info.properties.getProperty("java.opts", "");
            if (javaOpts.isBlank() == false) {
                extraJavaOptions.add(javaOpts);
            }
        };

        modulesInfo.forEach(infoConsumer);
        pluginInfo.forEach(infoConsumer);

        if (bootstrapJars.isEmpty() == false) {
            extraJavaOptions.add("-Xbootclasspath/a:" + String.join(":", bootstrapJars));
        }

        return extraJavaOptions;
    }

    // package-private for testing
    record PluginInfo(List<String> jarFiles, Properties properties) {}
}
