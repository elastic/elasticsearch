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
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

// TODO: 3 sets - testBuildInfo, internalClusterTest, external (as dependency of another)

public abstract class GenerateTestBuildInfoTask extends DefaultTask {

    public static final String DESCRIPTION = "generates plugin test dependencies file";
    public static final String PROPERTIES_FILENAME = "test-build-info.json";

    public GenerateTestBuildInfoTask() {
        setDescription(DESCRIPTION);
    }

    @Input
    public abstract Property<String> getComponentName();

    @InputFile
    @Optional
    public abstract RegularFileProperty getDescriptorFile();

    @InputFile
    @Optional
    public abstract RegularFileProperty getPolicyFile();

    @InputFiles
    public abstract Property<FileCollection> getCodeLocations();

    @OutputDirectory
    public abstract DirectoryProperty getOutputDirectory();

    @TaskAction
    public void generatePropertiesFile() throws IOException {
        Path outputDirectory = getOutputDirectory().get().getAsFile().toPath();
        Files.createDirectories(outputDirectory);
        Path outputFile = outputDirectory.resolve(PROPERTIES_FILENAME);

        try (var writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            writer.write("{\n");

            writer.write("    \"name\": \"");
            writer.write(getComponentName().get());
            writer.write("\",\n");

            writer.write("    \"descriptor\": \"");
            writer.write(getDescriptorFile().getAsFile().get().getAbsolutePath());
            writer.write("\",\n");

            if (getPolicyFile().isPresent()) {
                writer.write("    \"policy\": \"");
                writer.write(getPolicyFile().getAsFile().get().getAbsolutePath());
                writer.write("\",\n");
            }

            writer.write("    \"locations\": [\n");
            StringBuilder sb = new StringBuilder();
            for (File jar : getCodeLocations().get()) {
                sb.append("        \"");
                sb.append(jar.getAbsolutePath());
                sb.append("\",\n");
            }
            writer.write(sb.substring(0, sb.length() - 2));
            writer.write("\n    ]\n}\n");
        }
    }
}
