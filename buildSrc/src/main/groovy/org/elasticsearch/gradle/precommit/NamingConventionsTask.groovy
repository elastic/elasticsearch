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
import org.elasticsearch.gradle.VersionProperties
import org.gradle.api.artifacts.Dependency
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputFile

/**
 * Runs NamingConventionsCheck on a classpath/directory combo to verify that
 * tests are named according to our conventions so they'll be picked up by
 * gradle. Read the Javadoc for NamingConventionsCheck to learn more.
 */
public class NamingConventionsTask extends LoggedExec {
    /**
     * We use a simple "marker" file that we touch when the task succeeds
     * as the task output. This is compared against the modified time of the
     * inputs (ie the jars/class files).
     */
    @OutputFile
    File successMarker = new File(project.buildDir, 'markers/namingConventions')

    /**
     * The classpath to run the naming conventions checks against. Must contain the files in the test
     * output directory and everything required to load those classes.
     *
     * We don't declare the actual test files as a dependency or input because if they change then
     * this will change.
     */
    @InputFiles
    FileCollection classpath = project.sourceSets.test.runtimeClasspath

    /**
     * Should we skip the integ tests in disguise tests? Defaults to true because only core names its
     * integ tests correctly.
     */
    @Input
    boolean skipIntegTestInDisguise = false

    /**
     * Superclass for all tests.
     */
    @Input
    String testClass = 'org.apache.lucene.util.LuceneTestCase'

    /**
     * Superclass for all integration tests.
     */
    @Input
    String integTestClass = 'org.elasticsearch.test.ESIntegTestCase'

    public NamingConventionsTask() {
        // Extra classpath contains the actual test
        project.configurations.create('namingConventions')
        Dependency buildToolsDep = project.dependencies.add('namingConventions',
                "org.elasticsearch.gradle:build-tools:${VersionProperties.elasticsearch}")
        buildToolsDep.transitive = false // We don't need gradle in the classpath. It conflicts.
        FileCollection extraClasspath = project.configurations.namingConventions
        dependsOn(extraClasspath)

        description = "Runs NamingConventionsCheck on ${classpath}"
        executable = new File(project.javaHome, 'bin/java')
        onlyIf { project.sourceSets.test.output.classesDir.exists() }
        /*
         * We build the arguments in a funny afterEvaluate/doFirst closure so that we can wait for the classpath to be
         * ready for us. Strangely neither one on their own are good enough.
         */
        project.afterEvaluate {
            doFirst {
                args('-Djna.nosys=true')
                args('-cp', (classpath + extraClasspath).asPath, 'org.elasticsearch.test.NamingConventionsCheck')
                args(testClass, integTestClass)
                if (skipIntegTestInDisguise) {
                    args('--skip-integ-tests-in-disguise')
                }
                /*
                 * The test framework has classes that fail the checks to validate that the checks fail properly.
                 * Since these would cause the build to fail we have to ignore them with this parameter. The
                 * process of ignoring them lets us validate that they were found so this ignore parameter acts
                 * as the test for the NamingConventionsCheck.
                 */
                if (':build-tools'.equals(project.path)) {
                    args('--self-test')
                }
                args('--', project.sourceSets.test.output.classesDir.absolutePath)
            }
        }
        doLast { successMarker.setText("", 'UTF-8') }
    }
}
