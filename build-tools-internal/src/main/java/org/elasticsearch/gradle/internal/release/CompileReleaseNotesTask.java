/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import org.elasticsearch.gradle.Version;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFile;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.work.Incremental;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class CompileReleaseNotesTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(CompileReleaseNotesTask.class);

    private FileCollection inputFiles;
    private final RegularFileProperty outputFile;

    @Inject
    public CompileReleaseNotesTask(ObjectFactory objectFactory) {
        this.outputFile = objectFactory.fileProperty();
    }

    @Incremental
    @InputFiles
    public FileCollection getInputFiles() {
        return inputFiles;
    }

    public void setInputFiles(FileCollection inputFiles) {
        this.inputFiles = inputFiles;
    }

    @OutputFile
    public RegularFileProperty getOutputFile() {
        return this.outputFile;
    }

    public void setOutputFile(RegularFile file) {
        this.outputFile.set(file);
    }

    @TaskAction
    public void executeTask() throws IOException {
        final File output = this.outputFile.getAsFile().get();

        LOGGER.info("Concatenating files to " + output.getAbsolutePath());

        try (FileWriter writer = new FileWriter(output)) {

            List<File> sortedFiles = this.inputFiles.getFiles().stream()
                .filter(f -> f.getName().contains("-") == false)
                .sorted(Comparator.comparing(f -> Version.fromString(f.getName().replaceFirst("\\.asciidoc$", ""))))
                .collect(Collectors.toList());

            boolean first = true;
            for (File file : sortedFiles) {
                LOGGER.info("Concatenating " + file.getAbsolutePath());

                if (first) {
                    first = false;
                } else {
                    writer.write("\n\n");
                }

                writer.write(Files.readString(file.toPath()));
            }
        }
    }
}
