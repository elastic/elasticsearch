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
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

/**
 * A task to create a notice file which includes dependencies' notices.
 */
public class NoticeTask extends DefaultTask {

    @OutputFile
    File noticeFile = new File(project.buildDir, "notices/${name}/NOTICE.txt")

    /** Configurations to inspect dependencies*/
    private List<Project> dependencies = new ArrayList<>()

    public NoticeTask() {
        description = 'Create a notice file from dependencies'
    }

    /** Add notices from licenses found in the given project. */
    public void dependencies(Project project) {
        dependencies.add(project)
    }

    @TaskAction
    public void generateNotice() {
        StringBuilder output = new StringBuilder()
        output.append(project.rootProject.file('NOTICE.txt').getText('UTF-8'))
        output.append('\n\n')
        Set<String> seen = new HashSet<>()
        for (Project dep : dependencies) {
            File licensesDir = new File(dep.projectDir, 'licenses')
            if (licensesDir.exists() == false) continue
            licensesDir.eachFileMatch({ it ==~ /.*-NOTICE\.txt/ && seen.contains(it) == false}) { File file ->
                String name = file.name.substring(0, file.name.length() - '-NOTICE.txt'.length())
                appendFile(file, name, 'NOTICE', output)
                appendFile(new File(file.parentFile, "${name}-LICENSE.txt"), name, 'LICENSE', output)
                seen.add(file.name)
            }
        }
        noticeFile.setText(output.toString(), 'UTF-8')
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
