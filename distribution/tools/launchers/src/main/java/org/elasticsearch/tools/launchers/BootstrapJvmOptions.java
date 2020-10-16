/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tools.launchers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

/**
 * This class looks for plugins whose "type" is "bootstrap". Such plugins
 * will be added to the JVM's boot classpath. The plugins may also define
 * additional JVM options, in order to configure the bootstrap plugins.
 */
public class BootstrapJvmOptions {
    public static List<String> bootstrapJvmOptions(Path plugins) throws IOException {
        final File pluginsDir = Objects.requireNonNull(plugins).toFile();

        if (pluginsDir.isDirectory() == false) {
            throw new IllegalArgumentException("Plugins path " + plugins + " must be a directory");
        }

        final List<PluginInfo> pluginInfo = getPluginInfo(plugins);

        return generateOptions(pluginInfo);
    }

    // Find all plugins and return their jars and descriptors.
    private static List<PluginInfo> getPluginInfo(Path plugins) throws IOException {
        final File[] pluginDirectories = plugins.toFile().listFiles(File::isDirectory);

        Objects.requireNonNull(pluginDirectories);

        final List<PluginInfo> pluginInfo = new ArrayList<>();

        for (File pluginDir : pluginDirectories) {
            List<String> jarFiles = new ArrayList<>();
            Properties props = null;

            for (File pluginFile : Objects.requireNonNull(pluginDir.listFiles())) {
                final String lowerCaseName = pluginFile.getName().toLowerCase(Locale.ROOT);

                if (lowerCaseName.endsWith(".jar")) {
                    jarFiles.add(pluginFile.getAbsolutePath());
                } else if (lowerCaseName.equals("plugin-descriptor.properties")) {
                    props = new Properties();
                    try (InputStream stream = Files.newInputStream(pluginFile.toPath())) {
                        props.load(stream);
                    }
                }
            }

            if (props != null) {
                pluginInfo.add(new PluginInfo(jarFiles, props));
            }
        }

        return pluginInfo;
    }

    // package-private for testing
    static List<String> generateOptions(List<PluginInfo> pluginInfo) {
        final List<String> bootstrapJars = new ArrayList<>();
        final List<String> bootstrapOptions = new ArrayList<>();

        for (PluginInfo info : pluginInfo) {
            final String type = info.properties.getProperty("type", "isolated").toLowerCase(Locale.ROOT);

            if (type.equals("bootstrap")) {
                bootstrapJars.addAll(info.jarFiles);

                // Add any additional Java CLI options. This could contain any number of options,
                // but we don't attempt to split them up as all JVM options are concatenated together
                // anyway
                final String javaOpts = info.properties.getProperty("java.opts", "");
                if (javaOpts.isBlank() == false) {
                    bootstrapOptions.add(javaOpts);
                }
            }
        }

        if (bootstrapJars.isEmpty()) {
            return List.of();
        }

        bootstrapOptions.add("-Xbootclasspath/a:" + String.join(":", bootstrapJars));

        return bootstrapOptions;
    }

    // package-private for testing
    static class PluginInfo {
        public final List<String> jarFiles;
        public final Properties properties;

        PluginInfo(List<String> jarFiles, Properties properties) {
            this.jarFiles = jarFiles;
            this.properties = properties;
        }
    }
}
