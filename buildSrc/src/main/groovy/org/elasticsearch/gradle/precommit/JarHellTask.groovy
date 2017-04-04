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
import org.gradle.api.tasks.OutputFile

/**
 * Runs CheckJarHell on a classpath.
 */
public class JarHellTask extends LoggedExec {

    /**
     * We use a simple "marker" file that we touch when the task succeeds
     * as the task output. This is compared against the modified time of the
     * inputs (ie the jars/class files).
     */
    @OutputFile
    File successMarker = new File(project.buildDir, 'markers/jarHell')

    public JarHellTask() {
        project.afterEvaluate {
            FileCollection classpath = project.sourceSets.test.runtimeClasspath
            inputs.files(classpath)
            dependsOn(classpath)
            description = "Runs CheckJarHell on ${classpath}"
            executable = new File(project.javaHome, 'bin/java')
            doFirst({
                /* JarHell doesn't like getting directories that don't exist but
                  gradle isn't especially careful about that. So we have to do it
                  filter it ourselves. */
                FileCollection taskClasspath = classpath.filter { it.exists() }
                args('-cp', taskClasspath.asPath, 'org.elasticsearch.bootstrap.JarHell')
            })
            doLast({
                successMarker.parentFile.mkdirs()
                successMarker.setText("", 'UTF-8')
            })
        }
    }
}
