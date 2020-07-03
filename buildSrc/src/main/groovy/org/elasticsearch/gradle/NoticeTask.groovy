/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.gradle.api.file.FileCollection
import org.gradle.api.file.FileTree
import org.gradle.api.file.SourceDirectorySet
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

/**
 * A task to create a notice file which includes dependencies' notices.
 */
class NoticeTask extends DefaultTask {

    @InputFile
    File inputFile = project.rootProject.file('NOTICE.txt')

    @OutputFile
    File outputFile = new File(project.buildDir, "notices/${name}/NOTICE.txt")

    private FileTree sources

    /** Directories to include notices from */
    private List<File> licensesDirs = new ArrayList<>()

    NoticeTask() {
        description = 'Create a notice file from dependencies'
        // Default licenses directory is ${projectDir}/licenses (if it exists)
        File licensesDir = new File(project.projectDir, 'licenses')
        if (licensesDir.exists()) {
            licensesDirs.add(licensesDir)
        }
    }

    /** Add notices from the specified directory. */
    void licensesDir(File licensesDir) {
        licensesDirs.add(licensesDir)
    }

    void source(Object source) {
        if (sources == null) {
            sources = project.fileTree(source)
        } else {
            sources += project.fileTree(source)
        }
    }

    void source(SourceDirectorySet source) {
        if (sources == null) {
            sources = source
        } else {
            sources += source
        }
    }

    @TaskAction
    void generateNotice() {
        StringBuilder output = new StringBuilder()
        output.append(inputFile.getText('UTF-8'))
        output.append('\n\n')
        // This is a map rather than a set so that the sort order is the 3rd
        // party component names, unaffected by the full path to the various files
        Map<String, File> seen = new TreeMap<>()
        noticeFiles.each { File file ->
            String name = file.name.replaceFirst(/-NOTICE\.txt$/, "")
            if (seen.containsKey(name)) {
                File prevFile = seen.get(name)
                if (prevFile.text != file.text) {
                    throw new RuntimeException("Two different notices exist for dependency '" +
                            name + "': " + prevFile + " and " + file)
                }
            } else {
                seen.put(name, file)
            }
        }

        // Add all LICENSE and NOTICE files in licenses directory
        for (Map.Entry<String, File> entry : seen.entrySet()) {
            String name = entry.getKey()
            File file = entry.getValue()
            appendFile(file, name, 'NOTICE', output)
            appendFile(new File(file.parentFile, "${name}-LICENSE.txt"), name, 'LICENSE', output)
        }

        // Find any source files with "@notice" annotated license header
        for (File sourceFile : sources.files) {
            boolean isPackageInfo = sourceFile.name == 'package-info.java'
            boolean foundNotice = false
            boolean inNotice = false
            StringBuilder header = new StringBuilder()
            String packageDeclaration

            for (String line : sourceFile.readLines()) {
                if (isPackageInfo && packageDeclaration == null && line.startsWith('package')) {
                    packageDeclaration = line
                }

                if (foundNotice == false) {
                    foundNotice = line.contains('@notice')
                    inNotice = true
                } else {
                    if (line.contains('*/')) {
                        inNotice = false

                        if (!isPackageInfo) {
                            break
                        }
                    } else if (inNotice) {
                        header.append(line.stripMargin('*'))
                        header.append('\n')
                    }
                }
            }

            if (foundNotice) {
                appendText(header.toString(), isPackageInfo ? packageDeclaration : sourceFile.name, '', output)
            }
        }
        outputFile.setText(output.toString(), 'UTF-8')
    }

    @InputFiles
    @Optional
    FileCollection getNoticeFiles() {
        FileTree tree
        licensesDirs.each { dir ->
            if (tree == null) {
                tree = project.fileTree(dir)
            } else {
                tree += project.fileTree(dir)
            }
        }

        return tree?.matching { include '**/*-NOTICE.txt' }
    }

    @InputFiles
    @Optional
    FileCollection getSources() {
        return sources
    }

    static void appendFile(File file, String name, String type, StringBuilder output) {
        String text = file.getText('UTF-8')
        if (text.trim().isEmpty()) {
            return
        }
        appendText(text, name, type, output)
    }

    static void appendText(String text, String name, String type, StringBuilder output) {
        output.append('================================================================================\n')
        output.append("${name} ${type}\n")
        output.append('================================================================================\n')
        output.append(text)
        output.append('\n\n')
    }
}
