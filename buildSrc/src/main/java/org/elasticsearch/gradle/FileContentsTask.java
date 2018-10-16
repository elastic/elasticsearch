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

import static java.nio.file.Files.write;

import java.io.File;
import java.io.IOException;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

/**
 * Creates a file and sets it contents to something.
 */
public class FileContentsTask extends DefaultTask {

    /**
     * The file to be built.
     */
    @OutputFile
    private File file;

    @Input
    private String contents;


    /**
     * the contents to be written in the file
     */
    public void setContents(String contents) {
        this.contents = contents;
    }

    /**
     * The file to be built. Takes a String or a File and coerces it to a file.
     */
    public void setFile(File file) {
        this.file = file;
    }

    public void setFile(String file) {
        this.file = getProject().file(file);
    }

    @TaskAction
    public void writeFile() throws IOException {
        write(file.toPath(), contents.getBytes("UTF-8"));
    }
}
