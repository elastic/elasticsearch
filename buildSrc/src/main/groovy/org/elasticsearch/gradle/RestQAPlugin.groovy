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

import com.carrotsearch.gradle.randomizedtesting.RandomizedTestingTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.SourceSet

/** A basic elasticsearch build, along with rest integration tests. */
class RestQAPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        project.pluginManager.apply('java-base')
        project.pluginManager.apply('carrotsearch.randomizedtesting')

        // remove some unnecessary tasks for a qa test
        project.tasks.removeAll { it.name in ['assemble', 'buildDependents'] }
        project.sourceSets {
            test
        }
        project.dependencies {
            testCompile "org.elasticsearch:test-framework:${ElasticsearchProperties.version}"
        }
        Map properties = [
            name: 'integTest',
            type: RestIntegTestTask,
            dependsOn: 'testClasses',
            group: JavaBasePlugin.VERIFICATION_GROUP,
            description: 'Runs REST spec QA tests.'
        ]
        RandomizedTestingTask integTest = project.tasks.create(properties) {
            sysProp 'tests.rest.load_packaged', 'false'
        }
        SourceSet testSourceSet = project.sourceSets.test
        integTest.classpath = testSourceSet.runtimeClasspath
        integTest.testClassesDir = testSourceSet.output.classesDir
        integTest.dependsOn(RestSpecHack.setup(project, false))
        project.check.dependsOn(integTest)
    }
}
