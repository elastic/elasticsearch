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

import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.FileCollection
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.TaskContainer

/**
 * Validation tasks which should be run before committing. These run before tests.
 */
class PrecommitTasks {

    /** Adds a precommit task, which depends on non-test verification tasks. */
    static void configure(Project project) {
        List precommitTasks = [
                configureForbiddenApis(project),
                configureForbiddenPatterns(project.tasks),
                configureJarHell(project)]

        Map precommitOptions = [
                name: 'precommit',
                group: JavaBasePlugin.VERIFICATION_GROUP,
                description: 'Runs all non-test checks.',
                dependsOn: precommitTasks
        ]
        Task precommit = project.tasks.create(precommitOptions)
        project.check.dependsOn(precommit)

        // delay ordering relative to test tasks, since they may not be setup yet
        project.afterEvaluate {
            Task test = project.tasks.findByName('test')
            if (test != null) {
                test.mustRunAfter(precommit)
            }
            Task integTest = project.tasks.findByName('integTest')
            if (integTest != null) {
                integTest.mustRunAfter(precommit)
            }
        }
    }

    static Task configureForbiddenApis(Project project) {
        project.pluginManager.apply('de.thetaphi.forbiddenapis')
        project.forbiddenApis {
            internalRuntimeForbidden = true
            failOnUnsupportedJava = false
            bundledSignatures = ['jdk-unsafe', 'jdk-deprecated']
            signaturesURLs = [getClass().getResource('/forbidden/all-signatures.txt')]
            suppressAnnotations = ['**.SuppressForbidden']
        }
        project.tasks.findByName('forbiddenApisMain').configure {
            bundledSignatures += ['jdk-system-out']
            signaturesURLs += [
                    getClass().getResource('/forbidden/core-signatures.txt'),
                    getClass().getResource('/forbidden/third-party-signatures.txt')]
        }
        project.tasks.findByName('forbiddenApisTest').configure {
            signaturesURLs += [getClass().getResource('/forbidden/test-signatures.txt')]
        }
        Task forbiddenApis = project.tasks.findByName('forbiddenApis')
        forbiddenApis.group = "" // clear group, so this does not show up under verification tasks
        return forbiddenApis
    }

    static Task configureForbiddenPatterns(TaskContainer tasks) {
        Map options = [
                name: 'forbiddenPatterns',
                type: ForbiddenPatternsTask,
                description: 'Checks source files for invalid patterns like nocommits or tabs',
        ]
        return tasks.create(options) {
            rule name: 'nocommit', pattern: /nocommit/
            rule name: 'tab', pattern: /\t/
        }
    }

    /**
     * Adds a task to run jar hell before on the test classpath.
     *
     * We use a simple "marker" file that we touch when the task succeeds
     * as the task output. This is compared against the modified time of the
     * inputs (ie the jars/class files).
     */
    static Task configureJarHell(Project project) {
        File successMarker = new File(project.buildDir, 'markers/jarHell')
        Exec task = project.tasks.create(name: 'jarHell', type: Exec)
        FileCollection testClasspath = project.sourceSets.test.runtimeClasspath
        task.dependsOn(testClasspath)
        task.inputs.files(testClasspath)
        task.outputs.file(successMarker)
        task.executable = new File(project.javaHome, 'bin/java')
        task.doFirst({
            /* JarHell doesn't like getting directories that don't exist but
              gradle isn't especially careful about that. So we have to do it
              filter it ourselves. */
            def taskClasspath = testClasspath.filter { it.exists() }
            task.args('-cp', taskClasspath.asPath, 'org.elasticsearch.bootstrap.JarHell')
        })
        if (task.logger.isInfoEnabled() == false) {
            task.standardOutput = new ByteArrayOutputStream()
            task.errorOutput = task.standardOutput
            task.ignoreExitValue = true
            task.doLast({
                if (execResult.exitValue != 0) {
                    logger.error(standardOutput.toString())
                    throw new GradleException("JarHell failed")
                }
            })
        }
        task.doLast({
            successMarker.parentFile.mkdirs()
            successMarker.setText("", 'UTF-8')
        })
        return task
    }
}
