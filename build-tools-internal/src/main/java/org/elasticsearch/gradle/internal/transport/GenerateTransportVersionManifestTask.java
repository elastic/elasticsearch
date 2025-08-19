/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public abstract class GenerateTransportVersionManifestTask extends DefaultTask {
    @InputDirectory
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getDefinitionsDirectory();

    @OutputFile
    public abstract RegularFileProperty getManifestFile();

    @TaskAction
    public void generateTransportVersionManifest() throws IOException {
        Path definitionsDir = getDefinitionsDirectory().get().getAsFile().toPath();
        Path manifestFile = getManifestFile().get().getAsFile().toPath();
        if (getDefinitionsDirectory().isPresent() == false) {
            // no definitions to capture, remove this leniency once all branches have at least one version
            Files.writeString(manifestFile, "", StandardCharsets.UTF_8);
            return;
        }

        try (var writer = Files.newBufferedWriter(manifestFile)) {
            Files.walkFileTree(definitionsDir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                    String subPath = definitionsDir.relativize(path).toString().replace('\\', '/');
                    writer.write(subPath + "\n");
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }
}
