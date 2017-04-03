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
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

/**
 * A task to create a notice file which includes dependencies' notices
 * and, optionally, notices from a list of extra directories.
 */
public class NoticeTask extends DefaultTask {

    @InputFile
    File inputFile = project.rootProject.file('NOTICE.txt')

    @OutputFile
    File outputFile = new File(project.buildDir, "notices/${name}/NOTICE.txt")

    /** Configurations to inspect dependencies*/
    private List<Project> dependencies = new ArrayList<>()

    /** Extra directories to include notices from */
    @InputDirectory
    private List<File> extraLicensesDirs = new ArrayList<>()

    public NoticeTask() {
        description = 'Create a notice file from dependencies'
    }

    /** Add notices from licenses found in the given project. */
    public void dependencies(Project project) {
        dependencies.add(project)
    }

    /** Add notices from an extra directory. */
    public void extraLicensesDir(File extraLicensesDir) {
        extraLicensesDirs.add(extraLicensesDir)
    }

    @TaskAction
    public void generateNotice() {
        StringBuilder output = new StringBuilder()
        output.append(inputFile.getText('UTF-8'))
        output.append('\n\n')
        Map<String, File> seen = new TreeMap<>()
        for (Project dep : dependencies) {
            iterateLicensesDir(new File(dep.projectDir, 'licenses'), seen, true)
        }
        for (File extraLicensesDir : extraLicensesDirs) {
            iterateLicensesDir(extraLicensesDir, seen, false)
        }
        for (File file : seen.values()) {
            String name = file.name.substring(0, file.name.length() - '-NOTICE.txt'.length())
            appendFile(file, name, 'NOTICE', output)
            appendFile(new File(file.parentFile, "${name}-LICENSE.txt"), name, 'LICENSE', output)
        }
        outputFile.setText(output.toString(), 'UTF-8')
    }

    static void iterateLicensesDir(File licensesDir, Map<String, File> seen, boolean isOptional) {
        if (isOptional && licensesDir.exists() == false) return
        licensesDir.eachFileMatch({ it ==~ /.*-NOTICE\.txt/ }) { File file ->
            seen.put(file.name, file)
        }
    }

    static void appendFile(File file, String name, String type, StringBuilder output) {
        String text = file.getText('UTF-8')
        if (text.trim().isEmpty()) {
            return
        }
        output.append('================================================================================\n')
        output.append("${name} ${type}\n")
        output.append('================================================================================\n')
        output.append(text)
        output.append('\n\n')
    }
}
