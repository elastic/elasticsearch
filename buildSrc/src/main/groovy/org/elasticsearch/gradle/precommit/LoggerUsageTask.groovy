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

package org.elasticsearch.gradle.precommit

import org.elasticsearch.gradle.LoggedExec
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputFile

/**
 * Runs LoggerUsageCheck on a set of directories.
 */
public class LoggerUsageTask extends LoggedExec {

    /**
     * We use a simple "marker" file that we touch when the task succeeds
     * as the task output. This is compared against the modified time of the
     * inputs (ie the jars/class files).
     */
    private File successMarker = new File(project.buildDir, 'markers/loggerUsage')

    private FileCollection classpath;

    private FileCollection classDirectories;

    public LoggerUsageTask() {
        project.afterEvaluate {
            dependsOn(classpath)
            description = "Runs LoggerUsageCheck on ${classDirectories}"
            executable = new File(project.javaHome, 'bin/java')
            if (classDirectories == null) {
                // Default to main and test class files
                List files = []
                // But only if the source sets that will make them exist
                if (project.sourceSets.findByName("main")) {
                    files.add(project.sourceSets.main.output.classesDir)
                    dependsOn project.tasks.classes
                }
                if (project.sourceSets.findByName("test")) {
                    files.add(project.sourceSets.test.output.classesDir)
                    dependsOn project.tasks.testClasses
                }
                /* In an extra twist, it isn't good enough that the source set
                 * exists. Empty source sets won't make a classes directory
                 * which will cause the check to fail. We have to filter the
                 * empty directories out manually. This filter is done right
                 * before the actual logger usage check giving the rest of the
                 * build the opportunity to actually build the directory.
                 */
                classDirectories = project.files(files).filter { it.exists() }
            }
            doFirst({
                args('-cp', getClasspath().asPath, 'org.elasticsearch.test.loggerusage.ESLoggerUsageChecker')
                getClassDirectories().each {
                    args it.getAbsolutePath()
                }
            })
            doLast({
                successMarker.parentFile.mkdirs()
                successMarker.setText("", 'UTF-8')
            })
        }
    }

    @InputFiles
    FileCollection getClasspath() {
        return classpath
    }

    void setClasspath(FileCollection classpath) {
        this.classpath = classpath
    }

    @InputFiles
    FileCollection getClassDirectories() {
        return classDirectories
    }

    void setClassDirectories(FileCollection classDirectories) {
        this.classDirectories = classDirectories
    }

    @OutputFile
    File getSuccessMarker() {
        return successMarker
    }

    void setSuccessMarker(File successMarker) {
        this.successMarker = successMarker
    }
}
