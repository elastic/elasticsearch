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
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class GenerateTransportVersionManifestTask extends DefaultTask {
    @InputDirectory
    public abstract DirectoryProperty getDefinitionsDirectory();

    @OutputFile
    public abstract RegularFileProperty getManifestFile();

    @TaskAction
    public void generateTransportVersionManifest() throws IOException {
        Path definitionsDir = getDefinitionsDirectory().get().getAsFile().toPath();
        Path manifestFile = getManifestFile().get().getAsFile().toPath();
        try (var writer = Files.newBufferedWriter(manifestFile)) {
            try (var stream = Files.list(definitionsDir)) {
                for (String filename : stream.map(p -> p.getFileName().toString()).toList()) {
                    if (filename.equals(manifestFile.getFileName().toString())) {
                        // don't list self
                        continue;
                    }
                    writer.write(filename + "\n");
                }
            }
        }
    }
}
