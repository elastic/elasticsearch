/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashSet;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

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

    public void setFiles(FileTree files) {
        this.files = files;
    }

    @InputFiles
    public FileTree getFiles() { return files; }

    public void setHeaderLine(String headerLine) {
        this.headerLine = headerLine;
    }

    @Input
    @Optional
    public String getHeaderLine() { return headerLine; }

    public void setTarget(File target) {
        this.target = target;
    }

    @OutputFile
    public File getTarget() {
        return target;
    }

    @TaskAction
    public void concatFiles() throws IOException {
        if (getHeaderLine() != null) {
            Files.write(
                getTarget().toPath(),
                (getHeaderLine() + '\n').getBytes(StandardCharsets.UTF_8)
            );
        }

        // To remove duplicate lines
        LinkedHashSet<String> uniqueLines = new LinkedHashSet<>();
        for (File f : getFiles()) {
            uniqueLines.addAll(Files.readAllLines(f.toPath(), StandardCharsets.UTF_8));
        }
        Files.write(
            getTarget().toPath(), uniqueLines, StandardCharsets.UTF_8, StandardOpenOption.APPEND
        );
    }

}
