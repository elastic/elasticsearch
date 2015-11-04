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
package org.elasticsearch.gradle.test

import com.carrotsearch.gradle.junit4.RandomizedTestingTask
import org.elasticsearch.gradle.ElasticsearchProperties
import org.gradle.api.Plugin
import org.gradle.api.Project

/** Configures the build to have a rest integration test.  */
class RestTestPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        project.pluginManager.apply('java-base')
        project.pluginManager.apply('carrotsearch.randomized-testing')

        // remove some unnecessary tasks for a qa test
        project.tasks.removeAll { it.name in ['assemble', 'buildDependents'] }

        // only setup tests to build
        project.sourceSets {
            test
        }
        project.dependencies {
            testCompile "org.elasticsearch:test-framework:${ElasticsearchProperties.version}"
        }

        RandomizedTestingTask integTest = RestIntegTestTask.configure(project)
        RestSpecHack.configureDependencies(project)
        integTest.configure {
            classpath = project.sourceSets.test.runtimeClasspath
            testClassesDir project.sourceSets.test.output.classesDir
        }

        project.eclipse {
            classpath {
                sourceSets = [project.sourceSets.test]
                plusConfigurations = [project.configurations.testRuntime]
            }
        }
    }
}
