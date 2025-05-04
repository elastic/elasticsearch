/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.plugin;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileCollection;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;

public abstract class GenerateTestBuildInfoTask extends DefaultTask {

    public static final String DESCRIPTION = "generates plugin test dependencies file";
    public static final String PROPERTIES_FILENAME = "test-build-info.json";

    public GenerateTestBuildInfoTask() {
        setDescription(DESCRIPTION);
    }

    @Input
    public abstract Property<String> getComponentName();

    @InputFiles
    public abstract Property<FileCollection> getCodeLocations();

    @OutputDirectory
    public abstract DirectoryProperty getOutputDirectory();

    @TaskAction
    public void generatePropertiesFile() throws IOException {
        Map<String, String> classesToModules = new HashMap<>();
        for (File file : getCodeLocations().get().getFiles()) {
            if (file.exists()) {
                if (file.getName().endsWith(".jar")) {
                    try (JarFile jarFile = new JarFile(file)) {
                        jarFile.stream()
                            .filter(
                                je -> je.getName().startsWith("META-INF") == false
                                    && je.getName().equals("module-info.class") == false
                                    && je.getName().endsWith(".class")
                            )
                            .findFirst()
                            .ifPresent(entry -> classesToModules.put(entry.getName(), "module-goes-here"));
                    } catch (IOException ioe) {
                        throw new UncheckedIOException(ioe);
                    }
                } else if (file.isDirectory()) {
                    List<File> files = new ArrayList<>(List.of(file));
                    while (files.isEmpty() == false) {
                        File find = files.removeFirst();
                        if (find.exists()) {
                            if (find.isDirectory() && find.getName().equals("META_INF") == false) {
                                files.addAll(Arrays.asList(find.listFiles()));
                            } else if (find.getName().equals("module-info.class") == false && find.getName().contains("$") == false) {
                                classesToModules.put(find.getName(), "module-goes-here");
                                break;
                            }
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Unrecognized classpath file: " + file);
                }
            }
        }

        Path outputDirectory = getOutputDirectory().get().getAsFile().toPath();
        Files.createDirectories(outputDirectory);
        Path outputFile = outputDirectory.resolve(PROPERTIES_FILENAME);

        try (var writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            writer.write("{\n");

            writer.write("    \"name\": \"");
            writer.write(getComponentName().get());
            writer.write("\",\n");

            writer.write("    \"locations\": [\n");
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : classesToModules.entrySet()) {
                sb.append("        {\n");
                sb.append("            \"class\": \"");
                sb.append(entry.getKey());
                sb.append("\",\n            \"module\": \"");
                sb.append(entry.getValue());
                sb.append("\"\n        },\n");
            }
            writer.write(sb.substring(0, sb.length() - 2));
            writer.write("\n    ]\n}\n");
        }
    }
}
