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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
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

        final List<Path> descriptors = new ArrayList<>();

        Files.walkFileTree(plugins, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (Objects.requireNonNull(file).getFileName().toString().equals("plugin-descriptor.properties")) {
                    descriptors.add(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });

        final List<String> bootstrapJars = new ArrayList<>();
        final List<String> bootstrapOptions = new ArrayList<>();

        for (Path descriptor : descriptors) {
            final Properties props = new Properties();
            try (InputStream stream = Files.newInputStream(descriptor)) {
                props.load(stream);
            }
            final String type = props.getProperty("type", "isolated");

            if (type.equals("bootstrap")) {
                // Put all jars into the JVM boot classpath
                final File[] jarFiles = descriptor.getParent()
                    .toFile()
                    .listFiles((dir, name) -> name.toLowerCase(Locale.ROOT).endsWith(".jar"));

                for (File file : Objects.requireNonNull(jarFiles)) {
                    bootstrapJars.add(file.getAbsolutePath());
                }

                // Add any additional Java CLI options
                final String javaOpts = props.getProperty("java.opts", "");
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
}
