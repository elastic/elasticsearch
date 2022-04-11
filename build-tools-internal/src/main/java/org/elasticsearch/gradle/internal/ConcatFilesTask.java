/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Concatenates a list of files into one and removes duplicate lines.
 */
public class ConcatFilesTask extends DefaultTask {

    public ConcatFilesTask() {
        setDescription("Concat a list of files into one.");
    }

    /** List of files to concatenate */
    private FileTree files;

    /** line to add at the top of the target file */
    private String headerLine;

    private File target;

    private List<String> additionalLines = new ArrayList<>();

    public void setFiles(FileTree files) {
        this.files = files;
    }

    @InputFiles
    public FileTree getFiles() {
        return files;
    }

    public void setHeaderLine(String headerLine) {
        this.headerLine = headerLine;
    }

    @Input
    @Optional
    public String getHeaderLine() {
        return headerLine;
    }

    public void setTarget(File target) {
        this.target = target;
    }

    @OutputFile
    public File getTarget() {
        return target;
    }

    @Input
    public List<String> getAdditionalLines() {
        return additionalLines;
    }

    public void setAdditionalLines(List<String> additionalLines) {
        this.additionalLines = additionalLines;
    }

    @TaskAction
    public void concatFiles() throws IOException {
        if (getHeaderLine() != null) {
            Files.write(getTarget().toPath(), (getHeaderLine() + '\n').getBytes(StandardCharsets.UTF_8));
        }

        // To remove duplicate lines
        LinkedHashSet<String> uniqueLines = new LinkedHashSet<>();
        for (File f : getFiles()) {
            uniqueLines.addAll(Files.readAllLines(f.toPath(), StandardCharsets.UTF_8));
        }
        Files.write(getTarget().toPath(), uniqueLines, StandardCharsets.UTF_8, StandardOpenOption.APPEND);

        for (String additionalLine : additionalLines) {
            Files.write(getTarget().toPath(), (additionalLine + '\n').getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        }
    }

}
