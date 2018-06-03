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

class FeatureAwareTask extends LoggedExec {

    // we touch this when the task succeeds
    private File successMarker = new File(project.buildDir, 'markers/featureAware')

    private FileCollection classpath

    private FileCollection classDirectories

    FeatureAwareTask() {
        project.afterEvaluate {
            dependsOn classpath
            executable = new File(project.runtimeJavaHome, 'bin/java')
            if (classDirectories == null) {
                // default to main class files if such a source set exists
                final List files = []
                if (project.sourceSets.findByName("main")) {
                    files.add(project.sourceSets.main.output.classesDir)
                    dependsOn project.tasks.classes
                }
                // filter out non-existent classes directories from empty source sets
                classDirectories = project.files(files).filter { it.exists() }
            }
            description = "Runs FeatureAwareCheck on ${classDirectories.files}."
            doFirst {
                args('-cp', getClasspath().asPath, 'org.elasticsearch.xpack.test.feature_aware.FeatureAwareCheck')
                getClassDirectories().each { args it.getAbsolutePath() }
            }
            doLast {
                successMarker.parentFile.mkdirs()
                successMarker.setText("", 'UTF-8')
            }
        }
    }

    @InputFiles
    FileCollection getClasspath() {
        return classpath
    }

    void setClasspath(final FileCollection classpath) {
        this.classpath = classpath
    }

    @InputFiles
    FileCollection getClassDirectories() {
        return classDirectories
    }

    void setClassDirectories(final FileCollection classDirectories) {
        this.classDirectories = classDirectories
    }

    @OutputFile
    File getSuccessMarker() {
        return successMarker
    }

    void setSuccessMarker(final File successMarker) {
        this.successMarker = successMarker
    }

}
