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

package org.elasticsearch.gradle

import org.gradle.api.DefaultTask
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

/**
 * Concatenates a list of files into one.
 */
public class ConcatFilesTask extends DefaultTask {

    /** List of files to concatenate */
    @InputFiles
    FileTree files

    /** line to add at the top of the target file */
    @Input
    @Optional
    String headerLine

    @OutputFile
    File target

    public ConcatFilesTask() {
        description = 'Concat a list of files into one.'
    }

    @TaskAction
    public void concatFiles() {
        final StringBuilder output = new StringBuilder()

        if (headerLine) {
            output.append(headerLine).append('\n')
        }

        final File globalFile = File.createTempFile('global', 'txt')
        files.each {
            globalFile.append(it.getText('UTF-8'))
        }
        final TreeSet<String> lines = globalFile.readLines('UTF-8').toSet()
        for (String value : lines) {
            output.append(value).append('\n')
        }

        target.setText(output.toString(), 'UTF-8')
    }
}
