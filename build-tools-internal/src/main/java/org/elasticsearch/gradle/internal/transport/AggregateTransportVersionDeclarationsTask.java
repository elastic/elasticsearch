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
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class AggregateTransportVersionDeclarationsTask extends DefaultTask {

    @InputFiles
    public abstract ConfigurableFileCollection getTransportVersionNameDeclarationsFiles();

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @TaskAction
    public void aggregateTransportVersionDeclarations() {
        final var files = getTransportVersionNameDeclarationsFiles().getFiles();
        var allTVNames= files.stream().flatMap(file -> {
            try {
                return Files.lines(Path.of(file.getPath()));
            } catch (IOException e) {
                throw new RuntimeException("Cannot read Transport Versions name declarations file", e);
            }
        }).toList();

        File file = new File(getOutputFile().get().getAsFile().getAbsolutePath());
        try (FileWriter writer = new FileWriter(file)) {
            for (String tvName : allTVNames) {
                writer.write(tvName + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
