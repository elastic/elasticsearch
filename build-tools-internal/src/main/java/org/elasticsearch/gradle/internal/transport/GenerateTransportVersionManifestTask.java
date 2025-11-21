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
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public abstract class GenerateTransportVersionManifestTask extends DefaultTask {

    @ServiceReference("transportVersionResources")
    abstract Property<TransportVersionResourcesService> getTransportResources();

    @InputDirectory
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public Path getDefinitionsDirectory() {
        return getTransportResources().get().getDefinitionsDir();
    }

    @OutputFile
    public abstract RegularFileProperty getManifestFile();

    @TaskAction
    public void generateTransportVersionManifest() throws IOException {
        Path definitionsDir = getDefinitionsDirectory();
        Path manifestFile = getManifestFile().get().getAsFile().toPath();
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
