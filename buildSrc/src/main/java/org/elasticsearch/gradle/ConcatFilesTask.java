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

 import org.gradle.api.DefaultTask;
 import org.gradle.api.file.FileTree;
 import org.gradle.api.tasks.Input;
 import org.gradle.api.tasks.InputFiles;
 import org.gradle.api.tasks.Optional;
 import org.gradle.api.tasks.OutputFile;
 import org.gradle.api.tasks.TaskAction;

 import java.io.File;
 import java.io.IOException;
 import java.nio.charset.Charset;
 import java.nio.charset.StandardCharsets;
 import java.nio.file.Files;
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

    List<String> fileReadAllLines(File f, String encoding) {
        try{
            return Files.readAllLines(f.toPath(), Charset.forName(encoding));
        }
        catch (IOException e) {
            return new ArrayList<>();
        }
    }

    @TaskAction
    public void concatFiles() throws IOException {
        final String encoding = StandardCharsets.UTF_8.name();
        if (getHeaderLine() != null) {
            Files.write(target.toPath(), (getHeaderLine() + '\n').getBytes(encoding));
        }
        assert getHeaderLine() == "Header";

        // To remove duplicate lines
        LinkedHashSet<String> uniqueLines = new LinkedHashSet<>();
        getFiles().getFiles().forEach(f -> uniqueLines.addAll(fileReadAllLines(f, encoding)));
        Files.write(target.toPath(), uniqueLines, Charset.forName(encoding));
    }

    public void setTarget(File target) {
        this.target = target;
    }

    @OutputFile
    public File getTarget() {
        return target;
    }

}
