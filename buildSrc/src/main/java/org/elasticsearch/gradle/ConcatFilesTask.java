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

 import java.io.*;
 import java.nio.charset.StandardCharsets;
 import java.util.Arrays;
 import java.util.LinkedHashSet;
 import java.util.Scanner;

/**
 * Concatenates a list of files into one and removes duplicate lines.
 */
public class ConcatFilesTask extends DefaultTask {

    /** List of files to concatenate */
    @InputFiles
    public FileTree files;

    /** line to add at the top of the target file */
    @Input
    @Optional
    public String headerLine;

    @OutputFile
    public File target;

    public ConcatFilesTask() {
        final String description = "Concat a list of files into one.";
    }

    @TaskAction
    public void concatFiles() {
        final StringBuilder output = new StringBuilder();

        if (!headerLine.equals("")) {
            output.append(headerLine).append('\n');
        }

        final StringBuilder sb = new StringBuilder();
        for (File f: files) {
            try{
                Scanner scanner = new Scanner(f, "UTF-8" );
                String text = scanner.useDelimiter("\\A").next();
                scanner.close();
                sb.append(text);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        // Remove duplicate lines
        LinkedHashSet<String> uniqueLines = new LinkedHashSet<>(Arrays.asList(sb.toString().split("\n")));
        try {
            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(target), StandardCharsets.UTF_8);
            for (String s : uniqueLines) {
                osw.write(s);
                osw.write("\n");
            }
            osw.flush();
            osw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
